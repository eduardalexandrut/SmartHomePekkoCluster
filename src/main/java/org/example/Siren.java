package org.example;

import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.time.Duration;

/**
 * The siren. It registers itself with the control unit and repeats the
 * registration periodically: if the control unit is restarted or recreated on
 * another node, it has lost its list of sirens, and finds this siren again
 * within a few seconds.
 */
public class Siren extends AbstractActorWithTimers {
    private static final Object REGISTER_TIMER_KEY = "RegisterTimerKey";
    private static final Duration REGISTER_INTERVAL = Duration.ofSeconds(3);

    // Internal tick token for the periodic registration
    private static class RegisterTick {}

    private final ActorRef controlUnit;

    public static Props props(ActorRef controlUnit) {
        return Props.create(Siren.class, () -> new Siren(controlUnit));
    }

    public Siren(ActorRef controlUnit) {
        this.controlUnit = controlUnit;
    }

    @Override
    public void preStart() {
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(getSelf()), ActorRef.noSender());
        getTimers().startTimerWithFixedDelay(REGISTER_TIMER_KEY, new RegisterTick(), REGISTER_INTERVAL);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegisterTick.class, this::onRegisterTick)
                .match(SmartHomeProtocol.ActivateSiren.class, this::onActivateSiren)
                .match(SmartHomeProtocol.DeactivateSiren.class, this::onDeactivateSiren)
                .build();
    }

    private void onRegisterTick(RegisterTick registerTick) {
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(getSelf()), getSelf());
    }

    private void onDeactivateSiren(SmartHomeProtocol.DeactivateSiren deactivateSiren) {
        System.out.println("Siren deactivated!");
    }

    private void onActivateSiren(SmartHomeProtocol.ActivateSiren activateSiren) {
        System.out.println("Siren activated!");
    }

}