package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

public class Siren extends AbstractActor {
    public static Props props() {
        return Props.create(Siren.class, Siren::new);
    }

    @Override
    public void preStart() {
        System.out.println("[Siren Node] Registering hardware siren with Cluster Receptionist...");

        org.apache.pekko.actor.typed.ActorRef<Object> typedSelf = Adapter.toTyped(getSelf());
        org.apache.pekko.actor.ActorRef classicReceptionist = Adapter.toClassic(
                Receptionist.get(Adapter.toTyped(getContext().getSystem())).ref()
        );

        classicReceptionist.tell(
                Receptionist.register(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY, typedSelf),
                getSelf()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.ActivateSiren.class, this::onActivateSiren)
                .match(SmartHomeProtocolPekkoCluster.DeactivateSiren.class, this::onDeactivateSiren)
                .build();
    }

    private void onDeactivateSiren(SmartHomeProtocolPekkoCluster.DeactivateSiren deactivateSiren) {
        System.out.println("Siren deactivated!");
    }

    private void onActivateSiren(SmartHomeProtocolPekkoCluster.ActivateSiren activateSiren) {
        System.out.println("Siren activated!");
    }
}
