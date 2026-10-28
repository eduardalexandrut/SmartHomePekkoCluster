package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

/**
 * A motion sensor or a door/window sensor. Each sensor has a sensorId and the
 * zone it belongs to. When it detects something, it sends a SensorTriggered event
 * to the control unit, wherever the control unit is running in the cluster.
 */
public class Sensor extends AbstractActor {
    private final ActorRef controlUnit;
    private final String sensorId;
    private final String zone;

    public static Props props(String sensorId, String zone, ActorRef controlUnit) {
        return Props.create(Sensor.class, () -> new Sensor(sensorId, zone, controlUnit));
    }

    public Sensor(String sensorId, String zone, ActorRef controlUnit) {
        this.sensorId = sensorId;
        this.zone = zone;
        this.controlUnit = controlUnit;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocol.OpenWindowMsg.class, this::onWindowOpen)
                .match(SmartHomeProtocol.OpenDoorMsg.class, this::onDoorOpen)
                .match(SmartHomeProtocol.MotionDetectedMsg.class, this::onMotionDetected)
                .build();
    }

    private void onDoorOpen(SmartHomeProtocol.OpenDoorMsg openDoorMsg) {
        System.out.println("Sensor id: " + sensorId + " detected an open door event in " + zone);
        controlUnit.tell(new SmartHomeProtocol.SensorTriggered(sensorId, zone), getSelf());
    }

    private void onWindowOpen(SmartHomeProtocol.OpenWindowMsg openWindowMsg) {
        System.out.println("Sensor id: " + sensorId + " detected an open window event in " + zone);
        controlUnit.tell(new SmartHomeProtocol.SensorTriggered(sensorId, zone), getSelf());
    }

    private void onMotionDetected(SmartHomeProtocol.MotionDetectedMsg motionDetectedMsg) {
        System.out.println("Sensor id: " + sensorId + " detected a motion event in " + zone);
        controlUnit.tell(new SmartHomeProtocol.SensorTriggered(sensorId, zone), getSelf());
    }
}