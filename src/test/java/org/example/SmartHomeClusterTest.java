package org.example;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.example.TestCluster.ENTRY_DELAY;
import static org.example.TestCluster.EXIT_DELAY;
import static org.example.TestCluster.PIN;
import static org.example.TestCluster.sleep;
import static org.example.TestCluster.zones;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Alarm behavior on a real three-node cluster:
 *  - node 1 (control): the ControlUnit singleton
 *  - node 2 (sensors): the sensors of the house
 *  - node 3 (keypad):  the keypad, plus two test probes (siren and state queries)
 * Every message between sensors, keypad, control unit and siren crosses nodes.
 */
public class SmartHomeClusterTest {
    private static ActorSystem controlNode;
    private static ActorSystem sensorNode;
    private static ActorSystem keypadNode;

    private static Map<String, ActorRef> sensors;
    private static ActorRef keypad;
    private static ActorRef controlUnit; // proxy on the keypad node
    private static TestKit siren;
    private static TestKit states;

    @BeforeAll
    static void startCluster() {
        controlNode = TestCluster.startNode("control");
        sensorNode = TestCluster.startNode("sensors");
        keypadNode = TestCluster.startNode("keypad");
        TestCluster.formCluster(controlNode, sensorNode, keypadNode);

        Main.startControlUnitManager(controlNode);
        sensors = Main.startSensors(sensorNode, Main.startControlUnitProxy(sensorNode));
        controlUnit = Main.startControlUnitProxy(keypadNode);
        keypad = keypadNode.actorOf(KeyPad.props("test-keypad", controlUnit, PIN), "keypad");

        siren = new TestKit(keypadNode);
        states = new TestKit(keypadNode);
    }

    @AfterAll
    static void stopCluster() {
        TestKit.shutdownActorSystem(keypadNode);
        TestKit.shutdownActorSystem(sensorNode);
        TestKit.shutdownActorSystem(controlNode);
    }

    // Every test starts from Disarmed. A valid PIN leads to Disarmed from every state.
    @BeforeEach
    void resetToDisarmed() {
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(siren.getRef()), ActorRef.noSender());
        pin(PIN);
        awaitState("DISARMED");
        sleep(300);
        TestCluster.drain(siren);
    }

    @Test
    @DisplayName("A wrong PIN does not arm the system")
    void armingWithWrongPinIsRejected() {
        arm("0000");
        sleep(1000);
        assertEquals("DISARMED", state());
    }

    @Test
    @DisplayName("Correct PIN: Exit Delay, then Armed after the timeout")
    void armingGoesThroughExitDelay() {
        arm(PIN);
        awaitState("EXIT_DELAY");
        awaitState("ARMED");
    }

    @Test
    @DisplayName("Sensors are ignored during the Exit Delay")
    void sensorsIgnoredDuringExitDelay() {
        arm(PIN);
        awaitState("EXIT_DELAY");
        trigger("FrontDoor");
        awaitState("ARMED");
        sleep(500);
        assertEquals("ARMED", state());
        siren.expectNoMessage(Duration.ofMillis(500));
    }

    @Test
    @DisplayName("Sensors are only logged while Disarmed")
    void sensorsIgnoredWhenDisarmed() {
        trigger("FrontDoor");
        trigger("LivingRoomMotion");
        sleep(1000);
        assertEquals("DISARMED", state());
        siren.expectNoMessage(Duration.ofMillis(500));
    }

    @Test
    @DisplayName("Intrusion: Entry Delay, then Alarm; only the correct PIN stops it")
    void intrusionTriggersAlarmAfterEntryDelay() {
        arm(PIN);
        awaitState("ARMED");

        trigger("LivingRoomMotion");
        awaitState("ENTRY_DELAY");
        siren.expectNoMessage(Duration.ofMillis(700));    // not before the entry delay expires
        siren.expectMsgClass(Duration.ofSeconds(5), SmartHomeProtocol.ActivateSiren.class);
        awaitState("ALARM");

        trigger("FrontDoor");                              // already triggered: no effect
        pin("0000");                                       // wrong PIN: the alarm goes on
        sleep(500);
        assertEquals("ALARM", state());
        siren.expectNoMessage(Duration.ofMillis(500));

        pin(PIN);
        siren.expectMsgClass(Duration.ofSeconds(5), SmartHomeProtocol.DeactivateSiren.class);
        awaitState("DISARMED");
    }

    @Test
    @DisplayName("Correct PIN during the Entry Delay disarms the system")
    void validPinDuringEntryDelayDisarms() {
        arm(PIN);
        awaitState("ARMED");
        trigger("FrontDoor");
        awaitState("ENTRY_DELAY");
        pin(PIN);
        awaitState("DISARMED");
        siren.expectNoMessage(ENTRY_DELAY.plusSeconds(1));
    }

    @Test
    @DisplayName("Partial arming: sensors in inactive zones are ignored")
    void partialArmingIgnoresInactiveZones() {
        arm(PIN, "Perimeter", "GroundFloor");            // night mode: UpperFloor inactive
        awaitState("ARMED");

        trigger("BedroomMotion");
        sleep(700);
        assertEquals("ARMED", state());

        trigger("KitchenWindow");                         // Perimeter is active
        awaitState("ENTRY_DELAY");
    }

    @Test
    @DisplayName("Correct PIN disarms the system while Armed")
    void validPinDisarmsFromArmed() {
        arm(PIN);
        awaitState("ARMED");
        pin(PIN);
        awaitState("DISARMED");
    }

    @Test
    @DisplayName("Correct PIN during the Exit Delay cancels the arming")
    void validPinDuringExitDelayCancelsArming() {
        arm(PIN);
        awaitState("EXIT_DELAY");
        pin(PIN);
        awaitState("DISARMED");
        sleep(EXIT_DELAY.toMillis() + 500);
        assertEquals("DISARMED", state());
    }

    @Test
    @DisplayName("A restarted control unit enters recovery mode and needs the PIN")
    void restartedControlUnitEntersRecoveryMode() {
        arm(PIN);
        awaitState("ARMED");

        controlUnit.tell(new SmartHomeProtocol.SimulateCrash("test"), ActorRef.noSender());
        awaitState("RECOVERY");
        // the new instance lost its sirens: register again (a real Siren does it periodically)
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(siren.getRef()), ActorRef.noSender());

        // It does not assume Armed: an intrusion is only logged
        trigger("FrontDoor");
        siren.expectNoMessage(ENTRY_DELAY.plusSeconds(1));
        assertEquals("RECOVERY", state());

        // It does not assume Disarmed: arming is refused until the PIN is entered
        arm(PIN);
        sleep(700);
        assertEquals("RECOVERY", state());

        pin("0000");
        sleep(500);
        assertEquals("RECOVERY", state());

        pin(PIN);
        siren.expectMsgClass(Duration.ofSeconds(5), SmartHomeProtocol.DeactivateSiren.class);
        awaitState("DISARMED");

        // and from now on it works normally
        arm(PIN);
        awaitState("EXIT_DELAY");
    }

    // ---------- helpers ----------

    private static void arm(String pin, String... zones) {
        keypad.tell(new SmartHomeProtocol.ArmWithPin(pin, zones(zones)), ActorRef.noSender());
    }

    private static void pin(String pin) {
        keypad.tell(new SmartHomeProtocol.InsertPin(pin), ActorRef.noSender());
    }

    private static void trigger(String sensorId) {
        sensors.get(sensorId).tell(new SmartHomeProtocol.Detect(), ActorRef.noSender());
    }

    private static String state() {
        return TestCluster.currentState(controlUnit, states);
    }

    private static void awaitState(String expected) {
        TestCluster.awaitState(expected, controlUnit, states, Duration.ofSeconds(10));
    }
}
