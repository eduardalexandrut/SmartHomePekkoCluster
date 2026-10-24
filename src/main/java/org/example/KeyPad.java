package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

/**
 * Numeric keypad. It checks the PIN and forwards the result to the control unit
 * (through the singleton proxy, so the control unit can be on any node).
 */
public class KeyPad extends AbstractActor {
    private final String keypadId;
    private final ActorRef controlUnit;
    private final String correctPin;

    public static Props props(String keypadId, ActorRef controlUnit, String correctPin) {
        return Props.create(KeyPad.class, () -> new KeyPad(keypadId, controlUnit, correctPin));
    }

    public KeyPad(String keypadId, ActorRef controlUnit, String correctPin) {
        this.keypadId = keypadId;
        this.controlUnit = controlUnit;
        this.correctPin = correctPin;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocol.InsertPin.class, this::onInsertPin)
                .match(SmartHomeProtocol.ArmWithPin.class, this::onArmWithPin)
                .build();
    }

    // PIN typed to disarm the system or stop the alarm (or leave recovery mode)
    private void onInsertPin(SmartHomeProtocol.InsertPin insertPin) {
        if (insertPin.pin().equals(correctPin)) {
            System.out.println("[KeyPad " + keypadId + "] Correct PIN.");
            controlUnit.tell(new SmartHomeProtocol.ValidPinEntered(keypadId), getSelf());
        } else {
            System.out.println("[KeyPad " + keypadId + "] Wrong PIN!");
            controlUnit.tell(new SmartHomeProtocol.InvalidPinEntered(keypadId), getSelf());
        }
    }

    // The system is armed only if the PIN is correct
    private void onArmWithPin(SmartHomeProtocol.ArmWithPin armWithPin) {
        if (armWithPin.pin().equals(correctPin)) {
            System.out.println("[KeyPad " + keypadId + "] Correct PIN. Arming request sent.");
            controlUnit.tell(new SmartHomeProtocol.ArmSystemRequest(armWithPin.zonesToArm()), getSelf());
        } else {
            System.out.println("[KeyPad " + keypadId + "] Wrong PIN! The system is not armed.");
            controlUnit.tell(new SmartHomeProtocol.InvalidPinEntered(keypadId), getSelf());
        }
    }
}
