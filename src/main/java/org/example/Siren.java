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

    private static class RegisterTick {}

    private final ActorRef controlUnit;
    private boolean ringing = false;

    public static Props props(ActorRef controlUnit) {
        return Props.create(Siren.class, () -> new Siren(controlUnit));
    }

    public Siren(ActorRef controlUnit) {
        this.controlUnit = controlUnit;
    }

    @Override
    public void preStart() {
        register();
        getTimers().startTimerWithFixedDelay(REGISTER_TIMER_KEY, new RegisterTick(), REGISTER_INTERVAL);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegisterTick.class, m -> register())
                .match(SmartHomeProtocol.ActivateSiren.class, m -> {
                    if (!ringing) {
                        ringing = true;
                        System.out.println("[Siren] *** SIREN ACTIVATED ***");
                    }
                })
                .match(SmartHomeProtocol.DeactivateSiren.class, m -> {
                    if (ringing) {
                        ringing = false;
                        System.out.println("[Siren] Siren deactivated.");
                    }
                })
                .build();
    }

    private void register() {
        controlUnit.tell(new SmartHomeProtocol.RegisterSiren(getSelf()), getSelf());
    }
}
