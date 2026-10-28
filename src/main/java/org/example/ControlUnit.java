package org.example;

import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.japi.pf.ReceiveBuilder;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * The alarm control entity. It runs as a Cluster Singleton, so there
 * is exactly one in the cluster.
 *
 * Every new instance (first start, restart after a crash, re-creation on another
 * node) starts with empty fields, so it does not know the previous state: that is
 * why the initial state is RECOVERY, and only a correct PIN leads to DISARMED.
 */
public class ControlUnit extends AbstractActorWithTimers {
    private static final Object DELAY_TIMER_KEY = "DelayTimerKey";
    private final Set<String> activeZones = new HashSet<>(); // empty = all zones
    private final Set<ActorRef> sirens = new HashSet<>();

    // Internal tick tokens for timeouts
    private static class ExitDelayTimeout {}
    private static class EntryDelayTimeout {}

    private final Duration exitDelay;
    private final Duration entryDelay;

    public static Props props(Duration exitDelay, Duration entryDelay) {
        return Props.create(ControlUnit.class, () -> new ControlUnit(exitDelay, entryDelay));
    }

    public ControlUnit(Duration exitDelay, Duration entryDelay) {
        this.exitDelay = exitDelay;
        this.entryDelay = entryDelay;
    }

    @Override
    public void preStart() {
        System.out.println("[ControlUnit] Started in RECOVERY mode. Enter the PIN to disarm.");
    }

    @Override
    public Receive createReceive() {
        return recoveryState();
    }

    // Recovery state (initial state of every new instance):
    // - SensorTriggered
    // - ArmSystemRequest
    // - ValidPinEntered
    private Receive recoveryState() {
        return state(receiveBuilder()
                .match(SmartHomeProtocol.SensorTriggered.class, this::onSensorTriggeredRecovery)
                .match(SmartHomeProtocol.ArmSystemRequest.class, this::onArmSystemRequestRecovery)
                .match(SmartHomeProtocol.ValidPinEntered.class, this::onValidPinEnteredRecoveryState));
    }

    // Disarmed state:
    // - ArmSystemRequest
    // - SensorTriggered
    private Receive disarmedState() {
        return state(receiveBuilder()
                .match(SmartHomeProtocol.ArmSystemRequest.class, this::onArmSystemRequest)
                .match(SmartHomeProtocol.SensorTriggered.class, this::onSensorTriggeredDisarmed));
    }

    // Exit delay state:
    // - SensorTriggered
    // - ExitDelayTimeout
    private Receive exitDelayState() {
        return state(receiveBuilder()
                .match(ExitDelayTimeout.class, this::onExitDelayTimeout)
                .match(SmartHomeProtocol.SensorTriggered.class, this::onSensorTriggeredDisarmed));
    }

    // Armed state:
    // - SensorTriggered
    // - ValidPinEntered
    private Receive armedState() {
        return state(receiveBuilder()
                .match(SmartHomeProtocol.SensorTriggered.class, this::onSensorTriggeredArmed)
                .match(SmartHomeProtocol.ValidPinEntered.class, this::onValidPinEnteredArmedState));
    }

    // Entry delay state:
    // - ValidPinEntered
    // - EntryDelayTimeout
    private Receive entryDelayState() {
        return state(receiveBuilder()
                .match(SmartHomeProtocol.ValidPinEntered.class, this::onValidPinEnteredEntryState)
                .match(EntryDelayTimeout.class, this::onEntryDelayTimeout));
    }

    // Alarm state:
    // - ValidPinEntered
    // - InvalidPinEntered
    private Receive alarmState() {
        return state(receiveBuilder()
                .match(SmartHomeProtocol.ValidPinEntered.class, this::onValidPinEnteredAlarmState)
                .match(SmartHomeProtocol.InvalidPinEntered.class, this::onInvalidPinEnteredAlarmState));
    }

    // Messages handled in the same way in every state:
    // - RegisterSiren: a siren announces itself
    // - SimulateCrash: throws, so the supervisor restarts the actor (shows the recovery mode)
    private Receive state(ReceiveBuilder builder) {
        return builder
                .match(SmartHomeProtocol.RegisterSiren.class, this::onRegisterSiren)
                .match(SmartHomeProtocol.SimulateCrash.class, this::onSimulateCrash)
                .build();
    }

    private void onSensorTriggeredRecovery(SmartHomeProtocol.SensorTriggered sensorTriggered) {
        System.out.println("[ControlUnit] Recovery mode. Ignoring sensor trigger: " + sensorTriggered.sensorId());
    }

    private void onArmSystemRequestRecovery(SmartHomeProtocol.ArmSystemRequest armSystemRequest) {
        System.out.println("[ControlUnit] Recovery mode. Arming refused: enter the PIN first");
    }

    private void onValidPinEnteredRecoveryState(SmartHomeProtocol.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Leaving recovery mode");
        // in case an alarm was ringing when the previous instance was lost
        tellSirens(new SmartHomeProtocol.DeactivateSiren());
        getContext().become(disarmedState());
    }

    private void onInvalidPinEnteredAlarmState(SmartHomeProtocol.InvalidPinEntered invalidPinEntered) {
        System.out.println("[ControlUnit] WARNING! Invalid pin entered");
    }

    private void onValidPinEnteredAlarmState(SmartHomeProtocol.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Disarming alarm");
        tellSirens(new SmartHomeProtocol.DeactivateSiren());
        getContext().become(disarmedState());
    }

    private void onEntryDelayTimeout(EntryDelayTimeout entryDelayTimeout) {
        System.out.println("[ControlUnit] Entry delay timeout received. Setting up alarm!");
        tellSirens(new SmartHomeProtocol.ActivateSiren());
        getContext().become(alarmState());
    }

    private void onSensorTriggeredArmed(SmartHomeProtocol.SensorTriggered sensorTriggered) {
        String sensorZone = sensorTriggered.zone();

        // check if the zone is currently active (an empty set means full arming)
        if (activeZones.isEmpty() || activeZones.contains(sensorZone)) {
            System.out.println("[ControlUnit] WARNING! Intrusion detected Active zone breached: " + sensorZone);
            // Trigger entry delay and swap state
            getTimers().startSingleTimer(DELAY_TIMER_KEY, new EntryDelayTimeout(), entryDelay);
            getContext().become(entryDelayState());
        } else {
            System.out.println("[ControlUnit] Ignored sensor " + sensorTriggered.sensorId() + " (Zone " + sensorZone + " is inactive).");
        }
    }

    private void onValidPinEnteredEntryState(SmartHomeProtocol.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Disarming system");
        getTimers().cancel(DELAY_TIMER_KEY);
        getContext().become(disarmedState());
    }

    private void onValidPinEnteredArmedState(SmartHomeProtocol.ValidPinEntered validPinEntered) {
        System.out.println("[ControlUnit] Valid pin entered! Disarming system");
        getContext().become(disarmedState());
    }

    private void onExitDelayTimeout(ExitDelayTimeout exitDelayTimeout) {
        System.out.println("[ControlUnit] Exit delay expired. System is now armed.");
        getContext().become(armedState());
    }

    private void onSensorTriggeredDisarmed(SmartHomeProtocol.SensorTriggered sensorTriggered) {
        System.out.println("[ControlUnit] Disarmed. Logging sensor trigger: " + sensorTriggered.sensorId());
    }

    private void onArmSystemRequest(SmartHomeProtocol.ArmSystemRequest armSystemRequest) {
        System.out.println("[ControlUnit] Arming requested. Starting exit delay...");
        this.activeZones.clear();
        this.activeZones.addAll(armSystemRequest.zonesToArm());
        getTimers().startSingleTimer(DELAY_TIMER_KEY, new ExitDelayTimeout(), exitDelay);
        getContext().become(exitDelayState());
    }

    private void onRegisterSiren(SmartHomeProtocol.RegisterSiren registerSiren) {
        sirens.add(registerSiren.siren());
    }

    private void onSimulateCrash(SmartHomeProtocol.SimulateCrash simulateCrash) {
        throw new RuntimeException("Simulated crash: " + simulateCrash.reason());
    }

    private void tellSirens(Object message) {
        for (ActorRef siren : sirens) {
            siren.tell(message, self());
        }
    }
}