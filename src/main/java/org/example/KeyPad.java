package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.typed.javadsl.Adapter;

public class KeyPad extends AbstractActor {
    private final ActorRef controlUnitRouter;
    private static final String CORRECT_PIN = "1111";

    public static Props props() {
        return Props.create(KeyPad.class, KeyPad::new);
    }

    public KeyPad() {
        org.apache.pekko.actor.typed.Behavior<Object> groupBehavior =
                org.apache.pekko.actor.typed.javadsl.Routers.group(SmartHomeProtocolPekkoCluster.CONTROL_UNIT_SERVICE_KEY);

        org.apache.pekko.actor.typed.ActorRef<Object> typedRouter =
                Adapter.spawn(getContext(), groupBehavior, "controlUnitRouter");

        this.controlUnitRouter = Adapter.toClassic(typedRouter);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.InsertPinMsg.class, this::onInsertPinMsg)
                .build();
    }

    private void onInsertPinMsg(SmartHomeProtocolPekkoCluster.InsertPinMsg insertPinMsg) {
        final boolean isPinCorrect = insertPinMsg.pin().equals(CORRECT_PIN);
        if (isPinCorrect) {
            System.out.println("[KeyPad Node] Correct PIN entered! Forwarding to cluster Control Unit...");
            // Sends the message across the cluster safely via the router
            controlUnitRouter.tell(new SmartHomeProtocolPekkoCluster.ValidPinEntered(), getSelf());
        } else {
            System.out.println("[KeyPad Node] Wrong PIN entered!");
            controlUnitRouter.tell(new SmartHomeProtocolPekkoCluster.InvalidPinEntered(), getSelf());
        }
    }
}