package org.example;

import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import java.time.Duration;
import java.util.*;

public class ControlUnit extends AbstractActorWithTimers {
    private static final Object DELAY_TIMER_KEY = "DelayTimerKey";
    private final Map<String, String> sensorZoneMap;
    private final Set<String> activeZones = new HashSet<>();
    private ActorRef sirenRouter;

    private static class ExitDelayTimeout {}
    private static class EntryDelayTimeout {}

    private final java.time.Duration exitDelay;
    private final java.time.Duration entryDelay;

    public static Props props(Duration exitDelay, Duration entryDelay, Map<String, String> sensorZoneMap) {
        return Props.create(ControlUnit.class, () -> new ControlUnit(exitDelay, entryDelay, sensorZoneMap));
    }

    public ControlUnit(Duration exitDelay, Duration entryDelay, Map<String, String> sensorZoneMap) {
        this.exitDelay = exitDelay;
        this.entryDelay = entryDelay;
        this.sensorZoneMap = new HashMap<>(sensorZoneMap);
    }

    // Register this actor instance with the Cluster Registry
    @Override
    public void preStart() {
        System.out.println("[ControlUnit] Booting up. Registering with Cluster Receptionist...");

        org.apache.pekko.actor.typed.Behavior<Object> sirenGroup =
                org.apache.pekko.actor.typed.javadsl.Routers.group(SmartHomeProtocolPekkoCluster.SIREN_SERVICE_KEY);

        // 1. Spawn the typed group behavior
        org.apache.pekko.actor.typed.ActorRef<Object> typedSirenRouter =
                Adapter.spawn(getContext(), sirenGroup, "sirenRouter");

        // 2. Convert it to a classic ActorRef
        this.sirenRouter = Adapter.toClassic(typedSirenRouter);

        org.apache.pekko.actor.typed.ActorRef<Object> typedSelf = Adapter.toTyped(getSelf());

        ActorRef classicReceptionist = Adapter.toClassic(
                org.apache.pekko.actor.typed.receptionist.Receptionist.get(Adapter.toTyped(getContext().getSystem())).ref()
        );


        // 3. Register our ControlUnit key globally across the cluster
        classicReceptionist.tell(
                org.apache.pekko.actor.typed.receptionist.Receptionist.register(SmartHomeProtocolPekkoCluster.CONTROL_UNIT_SERVICE_KEY, typedSelf),
                getSelf()
        );

    }


    @Override
    public Receive createReceive() {
        return safeRecoveryModeState();
    }


    private Receive safeRecoveryModeState() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg.class, m -> {
                    System.out.println("[ControlUnit Recovery Log] Ignored cluster event from " + m.sensorId() + " in " + m.zoneName());
                })
                .match(SmartHomeProtocolPekkoCluster.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Pin verified. Exiting recovery mode baseline.");
                    getContext().become(disarmedState());
                })
                .build();
    }

    private Receive disarmedState() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.ArmSystemRequest.class, this::onArmSystemRequest)
                .match(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg.class, this::onSensorTriggeredDisarmed)
                .build();
    }

    private Receive exitDelayState() {
        return receiveBuilder()
                .match(ExitDelayTimeout.class, this::onExitDelayTimeout)
                .match(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg.class, this::onSensorTriggeredDisarmed)
                .build();
    }

    private Receive armedState() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg.class, this::onSensorTriggeredArmed)
                .build();
    }

    private Receive entryDelayState() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.ValidPinEntered.class, this::onValidPinEnteredEntryState)
                .match(EntryDelayTimeout.class, this::onEntryDelayTimeout)
                .build();
    }

    private Receive allarmState() {
        return receiveBuilder()
                .match(SmartHomeProtocolPekkoCluster.ValidPinEntered.class, this::onValidPinEnteredAllarmState)
                .match(SmartHomeProtocolPekkoCluster.InvalidPinEntered.class, this::onInvalidPinEnteredAllarmState)
                .build();
    }

    private void onInvalidPinEnteredAllarmState(SmartHomeProtocolPekkoCluster.InvalidPinEntered invalidPinEntered) {
        System.out.println("[ControlUnit] WARNING! Invalid pin entered: ");
    }

    private void onValidPinEnteredAllarmState(SmartHomeProtocolPekkoCluster.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Disarming allarm");
        sirenRouter.tell(new SmartHomeProtocolPekkoCluster.DeactivateSiren(), self());
        getContext().become(disarmedState());
    }

    private void onEntryDelayTimeout(EntryDelayTimeout entryDelayTimeout) {
        System.out.println("[ControlUnit] Entry delay timeout received. Setting up alarm!");
        sirenRouter.tell(new SmartHomeProtocolPekkoCluster.ActivateSiren(), this.self());
        getContext().become(allarmState());
    }

    private void onSensorTriggeredArmed(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg sensorTriggeredMsg) {
        String sensorZone = sensorZoneMap.get(sensorTriggeredMsg.sensorId());
        if (sensorZone != null && activeZones.contains(sensorZone)) {
            System.out.println("[ControlUnit] WARNING! Intrusion detected Active zone breached: " + sensorZone);
            getTimers().startSingleTimer(DELAY_TIMER_KEY, new EntryDelayTimeout(), entryDelay);
            getContext().become(entryDelayState());
        } else {
            System.out.println("[ControlUnit] Ignored sensor " + sensorTriggeredMsg.sensorId() + " (Zone " + sensorZone + " is inactive).");
        }
    }

    private void onValidPinEnteredEntryState(SmartHomeProtocolPekkoCluster.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Disarming system");
        getTimers().cancel(DELAY_TIMER_KEY);
        getContext().become(disarmedState());
    }

    private void onExitDelayTimeout(ExitDelayTimeout exitDelayTimeout) {
        System.out.println("[ControlUnit] Exit delay expired. System is now armed.");
        getContext().become(armedState());
    }

    private void onSensorTriggeredDisarmed(SmartHomeProtocolPekkoCluster.SensorTriggeredMsg sensorTriggeredMsg) {
        System.out.println("[ControlUnit] Disarmed. Logging sensor trigger: " + sensorTriggeredMsg.sensorId());
    }

    private void onArmSystemRequest(SmartHomeProtocolPekkoCluster.ArmSystemRequest armSystemRequest) {
        System.out.println("[ControlUnit] Arming requested. Starting exit delay...");
        this.activeZones.clear();
        this.activeZones.addAll(armSystemRequest.zonesToArm());
        getTimers().startSingleTimer(DELAY_TIMER_KEY, new ExitDelayTimeout(), exitDelay);
        getContext().become(exitDelayState());
    }
}