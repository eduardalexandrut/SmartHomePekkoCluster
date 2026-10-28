package org.example;

import org.apache.pekko.actor.ActorRef;

import java.util.Set;

public interface SmartHomeProtocol {

    // Messages sent to the ControlUnit actor (they cross the cluster)
    interface ControlUnitCommand extends CborSerializable {}

    // A sensor detected something. The sensor tells which zone it belongs to.
    record SensorTriggered(String sensorId, String zone) implements ControlUnitCommand {}

    // Sent by the keypad only after checking the PIN.
    // An empty set means full arming (all zones active).
    record ArmSystemRequest(Set<String> zonesToArm) implements ControlUnitCommand {
        public ArmSystemRequest {
            zonesToArm = Set.copyOf(zonesToArm); // messages must be immutable
        }
    }

    record ValidPinEntered(String keypadId) implements ControlUnitCommand {}
    record InvalidPinEntered(String keypadId) implements ControlUnitCommand {}

    // A siren announces itself to the control unit (it does so periodically, so a
    // restarted control unit, which has lost its state, finds it again).
    record RegisterSiren(ActorRef siren) implements ControlUnitCommand {}

    // Makes the control unit throw an exception, so that its supervisor restarts it.
    // Used to demonstrate the recovery mode.
    record SimulateCrash(String reason) implements ControlUnitCommand {}

    // Messages sent to the KeyPad actor
    interface KeyPadCommand extends CborSerializable {}
    record InsertPin(String pin) implements KeyPadCommand {}
    // Arming typed on the keypad: an empty set of zones means full arming
    record ArmWithPin(String pin, Set<String> zonesToArm) implements KeyPadCommand {
        public ArmWithPin {
            zonesToArm = Set.copyOf(zonesToArm);
        }
    }

    // Messages sent to a Sensor actor by its hardware
    interface SensorCommand extends CborSerializable {}
    record OpenWindowMsg() implements SensorCommand {}
    record OpenDoorMsg() implements SensorCommand {}
    record MotionDetectedMsg() implements SensorCommand {}

    // Messages sent to a Siren actor
    interface SirenCommand extends CborSerializable {}
    record ActivateSiren() implements SirenCommand {}
    record DeactivateSiren() implements SirenCommand {}
}