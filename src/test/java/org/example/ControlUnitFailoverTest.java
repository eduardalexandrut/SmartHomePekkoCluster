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
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

/**
 * The node running the ControlUnit leaves the cluster while the system is
 * Armed. The singleton is recreated on the other "control" node, without the
 * previous state, so it must start in recovery mode.
 *
 *  - node A (control):          hosts the ControlUnit first (oldest node)
 *  - node B (control, sensors): takes over the ControlUnit
 *  - node C (keypad):           keypad and the test probe that plays the siren
 */
public class ControlUnitFailoverTest {

    private ActorSystem nodeA;
    private ActorSystem nodeB;
    private ActorSystem nodeC;

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

        nodeA = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [control]").withFallback(config));
        nodeB = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [control, sensors]").withFallback(config));
        nodeC = ActorSystem.create("SmartHome", ConfigFactory.parseString("pekko.cluster.roles = [keypad]").withFallback(config));

        // Node A starts the cluster (so it is the oldest node), the others join it
        Address first = Cluster.get(nodeA).selfAddress();
        Cluster.get(nodeA).join(first);
        Cluster.get(nodeB).join(first);
        Cluster.get(nodeC).join(first);

        // Wait until the three nodes are Up
        new TestKit(nodeA).awaitCond(Duration.ofSeconds(30), () ->
                StreamSupport.stream(Cluster.get(nodeA).state().getMembers().spliterator(), false)
                        .filter(member -> member.status().equals(MemberStatus.up()))
                        .count() == 3);
    }

    @AfterEach
    public void tearDown() {
        TestKit.shutdownActorSystem(nodeC);
        TestKit.shutdownActorSystem(nodeB);
        TestKit.shutdownActorSystem(nodeA);
    }

    @Test
    @DisplayName("ControlUnit recreated on another node starts in recovery mode")
    public void testControlUnitRecreatedOnAnotherNode() throws Exception {
        // A singleton manager on both "control" nodes: the ControlUnit runs on A (the oldest)
        nodeA.actorOf(ClusterSingletonManager.props(
                        ControlUnit.props(FAST_EXIT_DELAY, FAST_ENTRY_DELAY),
                        PoisonPill.getInstance(),
                        ClusterSingletonManagerSettings.create(nodeA).withRole("control")),
                "controlUnit");
        nodeB.actorOf(ClusterSingletonManager.props(
                        ControlUnit.props(FAST_EXIT_DELAY, FAST_ENTRY_DELAY),
                        PoisonPill.getInstance(),
                        ClusterSingletonManagerSettings.create(nodeB).withRole("control")),
                "controlUnit");

        ActorRef sensorProxy = nodeB.actorOf(ClusterSingletonProxy.props("/user/controlUnit",
                ClusterSingletonProxySettings.create(nodeB).withRole("control")), "controlUnitProxy");
        ActorRef keypadProxy = nodeC.actorOf(ClusterSingletonProxy.props("/user/controlUnit",
                ClusterSingletonProxySettings.create(nodeC).withRole("control")), "controlUnitProxy");

        ActorRef frontDoor = nodeB.actorOf(Sensor.props("FrontDoor", "Perimeter", sensorProxy));
        ActorRef keypad = nodeC.actorOf(KeyPad.props("keypad", keypadProxy, PIN));
        final TestKit sirenProbe = new TestKit(nodeC);

        // Bring the system to Armed
        keypadProxy.tell(new SmartHomeProtocol.RegisterSiren(sirenProbe.getRef()), ActorRef.noSender());
        keypad.tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(20), SmartHomeProtocol.DeactivateSiren.class); // left RECOVERY
        keypad.tell(new SmartHomeProtocol.ArmWithPin(PIN, Set.of()), ActorRef.noSender());
        try { Thread.sleep(1500); } catch (InterruptedException e) {} // now Armed

        // Node A (running the ControlUnit) leaves the cluster
        Cluster.get(nodeA).leave(Cluster.get(nodeA).selfAddress());
        nodeA.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
        try { Thread.sleep(5000); } catch (InterruptedException e) {} // the ControlUnit is recreated on B

        // The new instance lost its sirens: register again
        keypadProxy.tell(new SmartHomeProtocol.RegisterSiren(sirenProbe.getRef()), ActorRef.noSender());

        // It does not assume the system is armed: an intrusion is only logged
        frontDoor.tell(new SmartHomeProtocol.OpenDoorMsg(), ActorRef.noSender());
        sirenProbe.expectNoMessage(Duration.ofSeconds(3));

        // The correct PIN leaves recovery mode (and switches the siren off)
        keypad.tell(new SmartHomeProtocol.InsertPin(PIN), ActorRef.noSender());
        sirenProbe.expectMsgClass(Duration.ofSeconds(10), SmartHomeProtocol.DeactivateSiren.class);
    }
}