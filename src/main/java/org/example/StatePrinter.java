package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;

/**
 * Small helper actor used by the console "status" command: it receives the
 * CurrentState reply of the control unit and prints it.
 */
public class StatePrinter extends AbstractActor {

    public static Props props() {
        return Props.create(StatePrinter.class, StatePrinter::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocol.CurrentState.class, m ->
                        System.out.println("[Status] Control unit state: " + m.state()))
                .build();
    }
}
