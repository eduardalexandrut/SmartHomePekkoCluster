
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SmartHomeTest {

    private ActorSystem system;

    // Fast delays for testing
    private final java.time.Duration fastExitDelay = java.time.Duration.ofMillis(100);
    private final java.time.Duration fastEntryDelay = java.time.Duration.ofMillis(100);

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
    private void registerMockWithReceptionist(ServiceKey<Object> key, ActorRef probeRef) {
        ActorRef classicReceptionist = Adapter.toClassic(Receptionist.get(Adapter.toTyped(system)).ref());
        classicReceptionist.tell(
                Receptionist.register(key, Adapter.toTyped(probeRef)),
                ActorRef.noSender()
        );
    }

    @Test
    public void testSirenFiresOnTimeout() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);

        // 1. Register our mock siren probe with the receptionist so the ControlUnit can route to it
        registerMockWithReceptionist(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY, sirenProbe.ref());

        Map<String, String> testConfig = Map.of("LivingRoomMotion", "GroundFloor");

        // 2. Spawn actors without passing hardcoded direct cross-references
        final ActorRef controlUnit = system.actorOf(ControlUnit.props(fastExitDelay, fastEntryDelay, testConfig), "controlUnit");
        final ActorRef motionSensor = system.actorOf(Sensor.props("LivingRoomMotion", "GroundFloor"), "motionSensor");
        final ActorRef keyPad = system.actorOf(KeyPad.props(), "keyPad");

        // Clear recovery mode on the ControlUnit first
//        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ValidPinEntered(), kit.testActor());
        keyPad.tell(new SmartHomeProtocolPekkoCluster.InsertPinMsg("1111"), kit.testActor());
        // Allow cluster routers a brief moment to discover the new keys
        try { Thread.sleep(250); } catch (InterruptedException e) {}

        // Arm the system
        Set<String> zonesToArm = Set.of("GroundFloor");
        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(zonesToArm), kit.testActor());

        // Wait out exit delay
        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Trigger the sensor
        motionSensor.tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Assert the Siren fires after the entry delay expires
        FiniteDuration assertionTimeout = JavaDurationConverters.asFiniteDuration(java.time.Duration.ofMillis(300));
        sirenProbe.expectMsgClass(assertionTimeout, SmartHomeProtocolPekkoCluster.ActivateSiren.class);
    }

    @Test
    public void testSuccessfulDisarmDuringEntryDelay() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);

        registerMockWithReceptionist(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY, sirenProbe.ref());

        Map<String, String> testConfig = Map.of("FrontDoor", "Perimeter");

        final ActorRef controlUnit = system.actorOf(ControlUnit.props(fastExitDelay, fastEntryDelay, testConfig), "controlUnit");
        final ActorRef frontDoorSensor = system.actorOf(Sensor.props("FrontDoor", "Perimeter"), "frontDoorSensor");

        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ValidPinEntered(), kit.testActor());
        try { Thread.sleep(200); } catch (InterruptedException e) {}

        Set<String> zonesToArm = Set.of("Perimeter");
        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(zonesToArm), kit.testActor());

        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Simulate intrusion
        frontDoorSensor.tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        // Simulate immediate disarm input
        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ValidPinEntered(), kit.testActor());

        // Assert that the Siren NEVER received an ActivateSiren command
        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(400, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }

    @Test
    public void testPartialArmingIgnoresInactiveZones() {
        final TestKit kit = new TestKit(system);
        final TestProbe sirenProbe = new TestProbe(system);

        registerMockWithReceptionist(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY, sirenProbe.ref());

        Map<String, String> testConfig = Map.of("BedroomMotion", "UpperFloor");

        final ActorRef controlUnit = system.actorOf(ControlUnit.props(fastExitDelay, fastEntryDelay, testConfig), "controlUnit");
        final ActorRef bedroomSensor = system.actorOf(Sensor.props("BedroomMotion", "UpperFloor"), "bedroomSensor");

        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ValidPinEntered(), kit.testActor());
        try { Thread.sleep(200); } catch (InterruptedException e) {}

        // Arm ONLY the Perimeter. Leave "UpperFloor" inactive!
        Set<String> nightModeZones = Set.of("Perimeter");
        controlUnit.tell(new SmartHomeProtocolPekkoCluster.ArmSystemRequest(nightModeZones), kit.testActor());

        try { Thread.sleep(150); } catch (InterruptedException e) {}

        // Trigger user movement upstairs in the inactive zone
        bedroomSensor.tell(new SmartHomeProtocolPekkoCluster.OpenDoorMsg(), kit.testActor());

        FiniteDuration safetyWindow = scala.concurrent.duration.Duration.create(400, TimeUnit.MILLISECONDS);
        sirenProbe.expectNoMessage(safetyWindow);
    }


}