
import jdk.jfr.Description;
import org.apache.pekko.actor.*;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;
import org.apache.pekko.testkit.TestKit;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.util.JavaDurationConverters;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.example.ControlUnit;
import org.example.KeyPad;
import org.example.Sensor;
import org.example.SmartHomeProtocolPekkoCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SmartHomeTest {

    private ActorSystem system;

    // Fast delays for rapid testing
    private static final Duration FAST_EXIT_DELAY = Duration.ofMillis(100);
    private static final Duration FAST_ENTRY_DELAY = Duration.ofMillis(100);

    // Shared home layout configuration
    private static final Map<String, String> TEST_CONFIG = Map.of(
            "LivingRoomMotion", "GroundFloor",
            "FrontDoor", "Perimeter",
            "BedroomMotion", "UpperFloor"
    );

    // Baseline configuration to turn a standard ActorSystem into a localized Cluster Node
    private Config getClusterConfig() {
        return ConfigFactory.parseString(
                "pekko.actor.provider = \"cluster\"\n" +
                        "pekko.remote.artery.canonical.port = 0\n" +
                        "pekko.remote.artery.canonical.hostname = \"127.0.0.1\"\n"
        );
    }

    @BeforeEach
    public void setup() {
        system = ActorSystem.create("TestAlarmSystem", getClusterConfig());
    }

    @AfterEach
    public void tearDown() {
        FiniteDuration duration = scala.concurrent.duration.Duration.create(2, TimeUnit.SECONDS);
        TestKit.shutdownActorSystem(system, duration, true);
    }

    // Helper method to register mock test probes with the Cluster Receptionist
    private void registerMockWithReceptionist(org.apache.pekko.actor.typed.receptionist.ServiceKey<Object> key, ActorRef probeRef) {
        ActorRef classicReceptionist = Adapter.toClassic(Receptionist.get(Adapter.toTyped(system)).ref());
        classicReceptionist.tell(
                Receptionist.register(key, Adapter.toTyped(probeRef)),
                ActorRef.noSender()
        );
    }

    /**
     * Helper record to package standard test actors together and reduce duplication.
     */
    private record TestEnvironment(
            ActorRef controlUnit,
            ActorRef frontDoorSensor,
            ActorRef livingRoomSensor,
            ActorRef bedroomSensor,
            ActorRef keypad
    ) {}

    /**
     * Spawns all actors, waits for cluster gossip to finish, and exits the safe recovery mode.
     */
    private TestEnvironment createEnvironment(TestKit kit, TestProbe sirenProbe) {
        // Register Mock Siren with Receptionist
        registerMockWithReceptionist(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY, sirenProbe.ref());

        // Spawn Distributed Actors
        ActorRef controlUnit = system.actorOf(ControlUnit.props(FAST_EXIT_DELAY, FAST_ENTRY_DELAY, TEST_CONFIG), "controlUnit");
        ActorRef frontDoor = system.actorOf(Sensor.props("FrontDoor", "Perimeter"), "frontDoor");
        ActorRef livingRoom = system.actorOf(Sensor.props("LivingRoomMotion", "GroundFloor"), "livingRoom");
        ActorRef bedroom = system.actorOf(Sensor.props("BedroomMotion", "UpperFloor"), "bedroom");
        ActorRef keypad = system.actorOf(KeyPad.props(), "keypad");

        // Wait for the Cluster Receptionist "Gossip" protocol to propagate routers
        try { Thread.sleep(1500); } catch (InterruptedException e) {}

        // Exit the Control Unit's Safe Recovery Mode
        keypad.tell(new SmartHomeProtocolPekkoCluster.InsertPinMsg("1111"), kit.testActor());

        // Give ControlUnit a tiny moment to process the PIN and shift to disarmedState
        try { Thread.sleep(50); } catch (InterruptedException e) {}

        return new TestEnvironment(controlUnit, frontDoor, livingRoom, bedroom, keypad);
    }

    @Test
    public void testSirenFiresOnTimeout() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        // Arm the system (GroundFloor & Perimeter)
        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("GroundFloor", "Perimeter")), kit.testActor());

        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Trigger a sensor in an active zone
        env.livingRoomSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Assert the Siren fires after the fast entry delay expires
        FiniteDuration assertionTimeout = JavaDurationConverters.asFiniteDuration(Duration.ofMillis(300));
        sirenProbe.expectMsgClass(assertionTimeout, SmartHomeProtocolPekkoCluster.ActivateSiren.class);
    }

    @Test
    public void testSuccessfulDisarmDuringEntryDelay() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("GroundFloor", "Perimeter", "UpperFloor")), kit.testActor());

        try { Thread.sleep(150); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        try { Thread.sleep(30); } catch (InterruptedException e) {}

        // Disarm via KeyPad cluster router
        env.keypad().tell(new SmartHomeProtocolPekkoCluster.InsertPinMsg("1111"), kit.testActor());

        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(500, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    public void testPartialArmingIgnoresInactiveZones() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        // Night Mode: Arm ONLY Perimeter and GroundFloor (UpperFloor left inactive)
        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("Perimeter", "GroundFloor")), kit.testActor());

        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Trigger user movement upstairs in the inactive zone
        env.bedroomSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(500, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    @Description("When the system is in the Disarmed state, triggering any sensor does not trigger an entry delay or fire the siren")
    public void testSensorsIgnoredWhenDisarmed() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        // Directly trigger sensor without arming
        env.bedroomSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(500, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    @Description("Verifies that sensors triggered while the exit delay countdown is active are ignored")
    public void testSensorsIgnoredDuringExitDelay() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("Perimeter")), kit.testActor());

        // Trigger sensor immediately during exit delay countdown
        env.frontDoorSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Wait out the exit delay
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Ensure siren never fired prematurely
        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(200, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    @Description("Verifies that entering a valid PIN during the Alarm state deactivates the siren and disarms the system")
    public void testAlarmStopsWithValidPin() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("Perimeter")), kit.testActor());
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Wait out entry delay to reach Alarm state
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Verify siren activated
        sirenProbe.expectMsgClass(SmartHomeProtocolPekkoCluster.ActivateSiren.class);

        // Enter valid PIN via keypad cluster router
        env.keypad().tell(new SmartHomeProtocolPekkoCluster.InsertPinMsg("1111"), kit.testActor());

        // Verify siren deactivated
        sirenProbe.expectMsgClass(SmartHomeProtocolPekkoCluster.DeactivateSiren.class);
    }

    @Test
    @Description("Verifies that entering an invalid PIN during the Alarm state does not stop the siren")
    public void testInvalidPinDuringAlarm() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("Perimeter")), kit.testActor());
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        env.frontDoorSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        sirenProbe.expectMsgClass(SmartHomeProtocolPekkoCluster.ActivateSiren.class);

        // Enter invalid PIN via keypad
        env.keypad().tell(new SmartHomeProtocolPekkoCluster.InsertPinMsg("0000"), kit.testActor());

        // Siren should keep going (expect no DeactivateSiren message)
        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(200, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    @Description("Verifies that full arming activates all zones, allowing sensors in upper floors to trigger the alarm")
    public void testFullArmingActivatesAllZones() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);
        final TestEnvironment env = createEnvironment(kit, sirenProbe);

        // Arm all zones including UpperFloor
        env.controlUnit().tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(Set.of("GroundFloor", "Perimeter", "UpperFloor")), kit.testActor());
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Trigger upper floor sensor
        env.bedroomSensor().tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Siren should fire after entry delay
        FiniteDuration assertionTimeout = JavaDurationConverters.asFiniteDuration(Duration.ofMillis(300));
        sirenProbe.expectMsgClass(assertionTimeout, SmartHomeProtocolPekkoCluster.ActivateSiren.class);
    }


}