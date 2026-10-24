package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.Member;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.testkit.javadsl.TestKit;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helpers to run several real cluster nodes (ActorSystems) inside the test JVM.
 * The nodes talk to each other over the network on localhost, so every message
 * between them goes through remoting and serialization, as in a real deployment.
 */
final class TestCluster {
    static final String PIN = "1111";
    static final Duration EXIT_DELAY = Duration.ofSeconds(2);
    static final Duration ENTRY_DELAY = Duration.ofSeconds(2);

    private TestCluster() {}

    static ActorSystem startNode(String... roles) {
        String roleList = Arrays.stream(roles).map(r -> "\"" + r + "\"").collect(Collectors.joining(","));
        Config config = ConfigFactory.parseString(
                        "pekko.remote.artery.canonical.port = 0\n" +     // any free port
                        "pekko.cluster.seed-nodes = []\n" +              // the test joins the nodes itself
                        "pekko.cluster.roles = [" + roleList + "]\n" +
                        "pekko.cluster.jmx.multi-mbeans-in-same-jvm = on\n" +
                        "pekko.loglevel = WARNING\n" +
                        "smarthome.exit-delay = " + EXIT_DELAY.toMillis() + "ms\n" +
                        "smarthome.entry-delay = " + ENTRY_DELAY.toMillis() + "ms\n")
                .withFallback(ConfigFactory.load());
        return ActorSystem.create("SmartHome", config);
    }

    // The first node starts the cluster (so it is the oldest one), then the
    // others join it. Returns when all the nodes are Up.
    static void formCluster(ActorSystem... nodes) {
        Address first = Cluster.get(nodes[0]).selfAddress();
        Cluster.get(nodes[0]).join(first);
        awaitMembersUp(nodes[0], 1);
        for (int i = 1; i < nodes.length; i++) {
            Cluster.get(nodes[i]).join(first);
        }
        awaitMembersUp(nodes[0], nodes.length);
    }

    static void awaitMembersUp(ActorSystem node, int expected) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            int up = 0;
            for (Member member : Cluster.get(node).state().getMembers()) {
                if (member.status().equals(MemberStatus.up())) {
                    up++;
                }
            }
            if (up == expected) {
                return;
            }
            sleep(200);
        }
        throw new AssertionError("The cluster did not reach " + expected + " members Up");
    }

    // Asks the control unit for its state, retrying while it is unreachable
    // (for example while it is being recreated).
    static String currentState(ActorRef controlUnitProxy, TestKit stateProbe) {
        for (int attempt = 0; attempt < 10; attempt++) {
            drain(stateProbe);
            controlUnitProxy.tell(new SmartHomeProtocol.GetState(stateProbe.getRef()), ActorRef.noSender());
            try {
                return stateProbe.expectMsgClass(Duration.ofSeconds(3), SmartHomeProtocol.CurrentState.class).state();
            } catch (AssertionError timeout) {
                // no answer yet: try again
            }
        }
        throw new AssertionError("The control unit does not answer");
    }

    static void awaitState(String expected, ActorRef controlUnitProxy, TestKit stateProbe, Duration max) {
        long deadline = System.currentTimeMillis() + max.toMillis();
        String last = null;
        while (System.currentTimeMillis() < deadline) {
            last = currentState(controlUnitProxy, stateProbe);
            if (last.equals(expected)) {
                return;
            }
            sleep(100);
        }
        throw new AssertionError("Expected state " + expected + " but it is " + last);
    }

    static void drain(TestKit probe) {
        while (probe.msgAvailable()) {
            probe.receiveOne(Duration.ofMillis(50));
        }
    }

    static Set<String> zones(String... zones) {
        return Set.of(zones);
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
