package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.PoisonPill;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.singleton.ClusterSingletonManager;
import org.apache.pekko.cluster.singleton.ClusterSingletonManagerSettings;
import org.apache.pekko.cluster.singleton.ClusterSingletonProxy;
import org.apache.pekko.cluster.singleton.ClusterSingletonProxySettings;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Alarm behavior on a real three-node cluster:
 *  - node 1 (control): the ControlUnit singleton
 *  - node 2 (sensors): the sensors of the house
 *  - node 3 (keypad):  the keypad, plus the test probe that plays the siren
 * Every message between sensors, keypad, control unit and siren crosses nodes.
 */
public class SmartHomeClusterTest {

    private ActorSystem controlNode;
    private ActorSystem sensorNode;
    private ActorSystem keypadNode;

    // Fast delays for rapid testing
    private static final Duration FAST_EXIT_DELAY = Duration.ofSeconds(1);
    private static final Duration FAST_ENTRY_DELAY = Duration.ofSeconds(1);
    private static final String PIN = "1111";

    @BeforeEach
    public void setup() {
        Config config = ConfigFactory.parseString(
                        "pekko.remote.artery.canonical.port = 0\n" +
                                "pekko.cluster.jmx.multi-mbeans-in-same-jvm = on\n" +
                                "pekko.loglevel = WARNING\n")
                .withFallback(ConfigFactory.load());

        controlNode = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [control]").withFallback(config));
        sensorNode = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [sensors]").withFallback(config));
        keypadNode = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [keypad]").withFallback(config));

        // The control node starts the cluster, the others join it
        Address first = Cluster.get(controlNode).selfAddress();
        Cluster.get(controlNode).join(first);
        Cluster.get(sensorNode).join(first);
        Cluster.get(keypadNode).join(first);

        // Wait until the three nodes are Up
        new TestKit(controlNode).awaitCond(Duration.ofSeconds(30), () ->
                StreamSupport.stream(Cluster.get(controlNode).state().getMembers().spliterator(), false)
                        .filter(member -> member.status().equals(MemberStatus.up()))
                        .count() == 3);
    }

    @AfterEach
    public void tearDown() {
        TestKit.shutdownActorSystem(keypadNode);
        TestKit.shutdownActorSystem(sensorNode);
        TestKit.shutdownActorSystem(controlNode);
    }

    /**
     * Helper record to package standard test actors together and reduce duplication.
     */
    private record TestEnvironment(
            ActorRef controlUnit,
            ActorRef frontDoorSensor,
            ActorRef kitchenWindowSensor,
            ActorRef livingRoomSensor,
            ActorRef bedroomSensor,
            ActorRef keypad
    ) {}

    private TestEnvironment createEnvironment(TestKit sirenProbe) {
        // ControlUnit singleton on the control node
        controlNode.actorOf(ClusterSingletonManager.props(
                        ControlUnit.props(FAST_EXIT_DELAY, FAST_ENTRY_DELAY),
                        PoisonPill.getInstance(),
                        ClusterSingletonManagerSettings.create(controlNode).withRole("control")),
                "controlUnit");

        // Proxies to the ControlUnit on the other two nodes
        ActorRef sensorProxy = sensorNode.actorOf(ClusterSingletonProxy.props("/user/controlUnit",
                ClusterSingletonProxySettings.create(sensorNode).withRole("control")), "controlUnitProxy");
        ActorRef keypadProxy = keypadNode.actorOf(ClusterSingletonProxy.props("/user/controlUnit",
                ClusterSingletonProxySettings.create(keypadNode).withRole("control")), "controlUnitProxy");

        ActorRef frontDoor = sensorNode.actorOf(Sensor.props("FrontDoor", "Perimeter", sensorProxy));
        ActorRef kitchenWindow = sensorNode.actorOf(Sensor.props("KitchenWindow", "Perimeter", sensorProxy));
        ActorRef livingRoom = sensorNode.actorOf(Sensor.props("LivingRoomMotion", "GroundFloor", sensorProxy));
        ActorRef bedroom = sensorNode.actorOf(Sensor.props("BedroomMotion", "UpperFloor", sensorProxy));
        ActorRef keypad = keypadNode.actorOf(KeyPad.props("keypad", keypadProxy, PIN));

        // The probe plays the siren
        keypadProxy.tell(new SmartHomeProtocol.RegisterSiren(sirenProbe.getRef()), ActorRef.noSender());

        // A new ControlUnit starts in RECOVERY: the PIN brings it to Disarmed (and switches the siren off)
        keypad.tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(20), SmartHomeProtocol.DeactivateSiren.class);

        return new TestEnvironment(keypadProxy, frontDoor, kitchenWindow, livingRoom, bedroom, keypad);
    }

    @Test
    @DisplayName("Intrusion in an active zone: the siren fires after the entry delay")
    public void testSirenFiresOnTimeout() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        // Arm the system (GroundFloor & Perimeter)
        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("GroundFloor", "Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        // Trigger a sensor in an active zone
        env.livingRoomSensor().tell(new SmartHomeProtocol.MotionDetectedMsg(), ActorRef.noSender());

        // The siren does not fire before the entry delay expires, but fires after it
        sirenProbe.expectNoMessage(Duration.ofMillis(600));
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);
    }

    @Test
    @DisplayName("A correct PIN during the entry delay disarms the system")
    public void testSuccessfulDisarmDuringEntryDelay() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("GroundFloor", "Perimeter", "UpperFloor")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        try { Thread.sleep(300); } catch (InterruptedException e) {}

        // Disarm via KeyPad
        env.keypad().tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());

        sirenProbe.expectNoMessage(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("Partial arming: sensors in inactive zones are ignored")
    public void testPartialArmingIgnoresInactiveZones() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        // Night Mode: Arm ONLY Perimeter and GroundFloor (UpperFloor left inactive)
        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter", "GroundFloor")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        // Trigger user movement upstairs in the inactive zone
        env.bedroomSensor().tell(new SmartHomeProtocol.MotionDetectedMsg(), ActorRef.noSender());
        sirenProbe.expectNoMessage(Duration.ofSeconds(2));

        // A window on the perimeter (active zone) does trigger the alarm
        env.kitchenWindowSensor().tell(new SmartHomeProtocol.OpenWindowMsg(), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);
    }

    @Test
    @DisplayName("When the system is Disarmed, sensors do not fire the siren")
    public void testSensorsIgnoredWhenDisarmed() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        env.bedroomSensor().tell(new SmartHomeProtocol.MotionDetectedMsg(), ActorRef.noSender());

        sirenProbe.expectNoMessage(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("Sensors triggered during the exit delay are ignored")
    public void testSensorsIgnoredDuringExitDelay() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(300); } catch (InterruptedException e) {}

        // Trigger sensor during the exit delay countdown
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());

        // Wait out the exit delay; the siren never fires
        try { Thread.sleep(1500); } catch (InterruptedException e) {}
        sirenProbe.expectNoMessage(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("A valid PIN during the Alarm deactivates the siren")
    public void testAlarmStopsWithValidPin() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());

        // Verify siren activated
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);

        // Enter valid PIN via keypad
        env.keypad().tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());

        // Verify siren deactivated
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.DeactivateSiren.class);
    }

    @Test
    @DisplayName("An invalid PIN during the Alarm does not stop the siren")
    public void testInvalidPinDuringAlarm() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);

        // Enter invalid PIN via keypad
        env.keypad().tell(new SmartHomeProtocol.InsertPin("0000"), ActorRef.noSender());

        // Siren should keep going (expect no DeactivateSiren message)
        sirenProbe.expectNoMessage(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("Full arming activates all zones, including the upper floor")
    public void testFullArmingActivatesAllZones() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        // No zones = full arming
        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of()), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        // Trigger upper floor sensor
        env.bedroomSensor().tell(new SmartHomeProtocol.MotionDetectedMsg(), ActorRef.noSender());

        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);
    }

    @Test
    @DisplayName("Arming with a wrong PIN leaves the system disarmed")
    public void testArmingWithWrongPinIsRejected() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin("0000", Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        // The system is still disarmed, so the sensor is ignored
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());

        sirenProbe.expectNoMessage(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("A valid PIN disarms the system while it is Armed")
    public void testDisarmFromArmedState() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {} // wait out the exit delay: now Armed

        // Disarm with the PIN
        env.keypad().tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        try { Thread.sleep(300); } catch (InterruptedException e) {}

        // The system is disarmed, so the sensor is ignored
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());

        sirenProbe.expectNoMessage(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("A restarted ControlUnit enters recovery mode and needs the PIN")
    public void testRecoveryAfterCrash() {
        final TestKit sirenProbe = new TestKit(keypadNode);
        final TestEnvironment env = createEnvironment(sirenProbe);

        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {} // now Armed

        // The ControlUnit crashes and is restarted: the new instance is in RECOVERY
        env.controlUnit().tell(new SmartHomeProtocol.SimulateCrash("test"), ActorRef.noSender());
        try { Thread.sleep(1000); } catch (InterruptedException e) {}

        // The new instance lost its sirens: register again (a real Siren does it periodically)
        env.controlUnit().tell(new SmartHomeProtocol.RegisterSiren(sirenProbe.getRef()), ActorRef.noSender());

        // It does not assume Armed: an intrusion is only logged
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        sirenProbe.expectNoMessage(Duration.ofSeconds(2));

        // It does not assume Disarmed: arming is refused
        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        sirenProbe.expectNoMessage(Duration.ofSeconds(2));

        // A wrong PIN has no effect
        env.keypad().tell(new SmartHomeProtocol.InsertPin("0000"), ActorRef.noSender());
        sirenProbe.expectNoMessage(Duration.ofSeconds(1));

        // The correct PIN leaves recovery mode (and switches the siren off)
        env.keypad().tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.DeactivateSiren.class);

        // And from now on it works normally
        env.keypad().tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of("Perimeter")), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {}
        env.frontDoorSensor().tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.ActivateSiren.class);
    }
}