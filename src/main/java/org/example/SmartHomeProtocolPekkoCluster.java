package org.example;

import org.apache.pekko.actor.typed.receptionist.ServiceKey;


import java.util.Set;
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public interface SmartHomeProtocolPekkoCluster {

    ServiceKey<Object> CONTROL_UNIT_SERVICE_KEY =
            ServiceKey.create(Object.class, "centralControlUnit");

    ServiceKey<Object> SIREN_SERVICE_KEY =
            ServiceKey.create(Object.class, "alarmSiren");

    // Messages sent to the KeyPad Actor
    interface KeyPadCommand {}
    record InsertPinMsg(String pin) implements KeyPadCommand {}

    // Messages sent to the ControlUnit Actor
    interface ControlUnitCommand {}
    record ValidPinEntered() implements ControlUnitCommand {}
    record InvalidPinEntered() implements ControlUnitCommand {}
    record SensorTriggeredMsg(String sensorId, String zoneName) implements ControlUnitCommand {}
    record ArmSystemRequest(Set<String> zonesToArm) implements ControlUnitCommand {}
    record DelayTimeout() implements ControlUnitCommand {}

    // Message sent to the Sensor actor
    interface SensorUnitCommand {}
    record OpenWindowMsg() implements SensorUnitCommand {}
    record OpenDoorMsg() implements SensorUnitCommand {}

    // Messages sent to the siren
    interface SirenCommand {}
    record ActivateSiren() implements SirenCommand {}
    record DeactivateSiren() implements SirenCommand {}
}