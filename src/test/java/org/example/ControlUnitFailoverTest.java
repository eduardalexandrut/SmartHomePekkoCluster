package org.example;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.example.TestCluster.PIN;
import static org.example.TestCluster.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The node running the ControlUnit leaves the cluster while the system is
 * Armed. The singleton is recreated on the other "control" node, without the
 * previous state, so it must start in recovery mode.
 *
 *  - node A (control):          hosts the ControlUnit first (oldest node)
 *  - node B (control, sensors): takes over the ControlUnit
 *  - node C (keypad):           keypad and test probes
 */
public class ControlUnitFailoverTest {
    private static ActorSystem nodeA;
    private static ActorSystem nodeB;
    private static ActorSystem nodeC;

    private static Map<String, ActorRef> sensors;
    private static ActorRef keypad;
    private static ActorRef controlUnit;
    private static TestKit siren;
    private static TestKit states;

    @BeforeAll
    static void startCluster() {
        nodeA = TestCluster.startNode("control");
        nodeB = TestCluster.startNode("control", "sensors");
        nodeC = TestCluster.startNode("keypad");
        TestCluster.formCluster(nodeA, nodeB, nodeC);

        Main.startControlUnitManager(nodeA);
        Main.startControlUnitManager(nodeB);
        sensors = Main.startSensors(nodeB, Main.startControlUnitProxy(nodeB));
        controlUnit = Main.startControlUnitProxy(nodeC);
        keypad = nodeC.actorOf(KeyPad.props("test-keypad", controlUnit, PIN), "keypad");

        siren = new TestKit(nodeC);
        states = new TestKit(nodeC);
    }

    @AfterAll
    static void stopCluster() {
        TestKit.shutdownActorSystem(nodeC);
        TestKit.shutdownActorSystem(nodeB);
        TestKit.shutdownActorSystem(nodeA);
    }

    @Test
    @DisplayName("ControlUnit recreated on another node starts in recovery mode")
    void recreatedControlUnitStartsInRecoveryMode() throws Exception {
        // Bring the system to Armed
        keypad.tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        awaitState("DISARMED", Duration.ofSeconds(15));
        keypad.tell(new SmartHomeProtocol.ArmWithPin(PIN, TestCluster.zones()), ActorRef.noSender());
        awaitState("ARMED", Duration.ofSeconds(10));

        // Node A (running the ControlUnit) leaves the cluster
        Cluster.get(nodeA).leave(Cluster.get(nodeA).selfAddress());
        nodeA.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);

        // The new instance on node B has no previous state
        awaitState("RECOVERY", Duration.ofSeconds(30));
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(siren.getRef()), ActorRef.noSender());

        // It does not assume the system is armed
        sensors.get("FrontDoor").tell(new SmartHomeProtocol.Detect(), ActorRef.noSender());
        siren.expectNoMessage(TestCluster.ENTRY_DELAY.plusSeconds(1));
        assertEquals("RECOVERY", TestCluster.currentState(controlUnit, states));

        // A correct PIN brings it back to Disarmed
        keypad.tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        siren.expectMsgClass(Duration.ofSeconds(5), SmartHomeProtocol.DeactivateSiren.class);
        awaitState("DISARMED", Duration.ofSeconds(10));
        sleep(100);
    }

    private static void awaitState(String expected, Duration max) {
        TestCluster.awaitState(expected, controlUnit, states, max);
    }
}
