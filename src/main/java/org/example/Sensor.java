package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.receptionist.Receptionist;

public class Sensor extends AbstractActor {
    private final String sensorId;
    private final String zoneName;
    private final ActorRef controlUnitRouter;

    public static Props props(String sensorId, String zoneName) {
        return Props.create(Sensor.class, () -> new Sensor(sensorId, zoneName));
    }

    public Sensor(String sensorId, String zoneName) {
        this.sensorId = sensorId;
        this.zoneName = zoneName;

        // Create a local Group Router instance pointing to the global ControlUnit key.
        // The router will automatically handle finding the ControlUnit node across the cluster.
//        org.apache.pekko.actor.typed.Behavior<Object> groupBehavior =
//                org.apache.pekko.actor.typed.javadsl.Routers.group(SmartHomeProtocolPekkoCluster.CONTROL_UNIT_SERVICE_KEY);
//
//        this.controlUnitRouter = getContext().actorOf(Adapter.toClassic(groupBehavior), "controlUnitRouter");

        org.apache.pekko.actor.typed.Behavior<Object> groupBehavior =
                org.apache.pekko.actor.typed.javadsl.Routers.group(SmartHomeProtocolPekkoCluster.CONTROL_UNIT_SERVICE_KEY);

        org.apache.pekko.actor.typed.ActorRef<Object> typedRouter =
                Adapter.spawn(getContext(), groupBehavior, "controlUnitRouter");

        this.controlUnitRouter = Adapter.toClassic(typedRouter);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.OpenDoorMsg.class, m -> {
                    System.out.println("[" + zoneName + " Node] Hardware sensor " + sensorId + " tripped!");

                    // Fire the event directly into the group router, forwarding it to the Control Unit node
                    controlUnitRouter.tell(
                            new SmartHomeProtocolPekkoCluster.SensorTriggeredMsg(sensorId, zoneName),
                            getSelf()
                    );
                })
                .build();
    }

    private void onDoorOpen(SmartHomeProtocolPekkoCluster.OpenDoorMsg openDoorMsg) {
        System.out.println("Sensor id: " + sensorId + " detected an open door event in " + zoneName);
        getSender().tell(new SmartHomeProtocolPekkoCluster.SensorTriggeredMsg(sensorId, zoneName), getSelf());
    }

    private void onWindowOpen(SmartHomeProtocolPekkoCluster.OpenWindowMsg triggerSensorMsg) {
        System.out.println("Sensor id: " + sensorId + " detected an open window event in " + zoneName);
        getSender().tell(new SmartHomeProtocolPekkoCluster.SensorTriggeredMsg(sensorId, zoneName), getSelf());
    }
}