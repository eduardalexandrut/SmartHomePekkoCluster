package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

/**
 * A motion sensor or a door/window sensor. Each sensor has a sensorId, a type
 * and the zone it belongs to. When its hardware detects something (Detect
 * message), it sends a SensorTriggered event to the control unit, wherever the
 * control unit is running in the cluster.
 */
public class Sensor extends AbstractActor {
    private final String sensorId;
    private final SmartHomeProtocol.SensorType type;
    private final String zone;
    private final ActorRef controlUnit;

    public static Props props(String sensorId, SmartHomeProtocol.SensorType type, String zone, ActorRef controlUnit) {
        return Props.create(Sensor.class, () -> new Sensor(sensorId, type, zone, controlUnit));
    }

    public Sensor(String sensorId, SmartHomeProtocol.SensorType type, String zone, ActorRef controlUnit) {
        this.sensorId = sensorId;
        this.type = type;
        this.zone = zone;
        this.controlUnit = controlUnit;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocol.Detect.class, this::onDetect)
                .build();
    }

    private void onDetect(SmartHomeProtocol.Detect detect) {
        String what = switch (type) {
            case MOTION -> "motion detected";
            case DOOR -> "door opened";
            case WINDOW -> "window opened";
        };
        System.out.println("[Sensor " + sensorId + "] " + what + " in zone " + zone);
        controlUnit.tell(new SmartHomeProtocol.SensorTriggered(sensorId, type, zone), getSelf());
    }
}
