package org.example;

import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.japi.pf.ReceiveBuilder;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * The alarm control entity.
 *
 * It runs as a Cluster Singleton (see Main): exactly one instance exists in the
 * cluster, on a node with the "control" role. Sensors, keypads and sirens reach
 * it through a ClusterSingletonProxy, wherever it is running.
 *
 * Every time an instance is created (first start, restart after a crash, or
 * re-creation on another node after a failure) all its fields start empty, so it
 * does not know whether the system was armed or not. For this reason the
 * initial behavior is the SAFE RECOVERY MODE, and only a correct PIN leads to
 * DISARMED.
 */
public class ControlUnit extends AbstractActorWithTimers {
    private static final Object DELAY_TIMER_KEY = "DelayTimerKey";

    // Internal tick tokens for timeouts (local messages, never sent to other nodes)
    private static class ExitDelayTimeout {}
    private static class EntryDelayTimeout {}

    private final Duration exitDelay;
    private final Duration entryDelay;

    private final Set<String> activeZones = new HashSet<>();
    private boolean fullArming = false;
    private final Set<ActorRef> sirens = new HashSet<>();
    private String stateName = "RECOVERY";

    public static Props props(Duration exitDelay, Duration entryDelay) {
        return Props.create(ControlUnit.class, () -> new ControlUnit(exitDelay, entryDelay));
    }

    public ControlUnit(Duration exitDelay, Duration entryDelay) {
        this.exitDelay = exitDelay;
        this.entryDelay = entryDelay;
    }

    @Override
    public void preStart() {
        System.out.println("[ControlUnit] Started on " + Cluster.get(getContext().getSystem()).selfAddress()
                + ". Previous state unknown: SAFE RECOVERY MODE. Enter the PIN to disarm.");
    }

    @Override
    public Receive createReceive() {
        return recoveryState();
    }

    // Recovery state (initial state of every new instance):
    // - SensorTriggered: logged, nothing else
    // - ArmSystemRequest: ignored, the PIN must first bring the system to Disarmed
    // - ValidPinEntered: -> Disarmed (and the sirens are switched off, in case an
    //   alarm was ringing when the previous instance was lost)
    private Receive recoveryState() {
        return withCommonMessages(receiveBuilder()
                .match(SmartHomeProtocol.SensorTriggered.class, m ->
                        System.out.println("[ControlUnit] RECOVERY: logged event from " + m.sensorId() + " (" + m.zone() + "), ignored."))
                .match(SmartHomeProtocol.ArmSystemRequest.class, m ->
                        System.out.println("[ControlUnit] RECOVERY: arming refused, enter the PIN to disarm first."))
                .match(SmartHomeProtocol.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Valid PIN: leaving recovery mode.");
                    tellSirens(new SmartHomeProtocol.DeactivateSiren());
                    goTo("DISARMED", disarmedState());
                }));
    }

    // Disarmed state:
    // - ArmSystemRequest: -> Exit delay
    // - SensorTriggered: logged
    private Receive disarmedState() {
        return withCommonMessages(receiveBuilder()
                .match(SmartHomeProtocol.ArmSystemRequest.class, this::onArmSystemRequest)
                .match(SmartHomeProtocol.SensorTriggered.class, this::logIgnoredSensor));
    }

    // Exit delay state:
    // - ExitDelayTimeout: -> Armed
    // - SensorTriggered: logged (sensors are not active yet)
    // - ValidPinEntered: arming cancelled, -> Disarmed
    private Receive exitDelayState() {
        return withCommonMessages(receiveBuilder()
                .match(ExitDelayTimeout.class, m -> goTo("ARMED", armedState()))
                .match(SmartHomeProtocol.SensorTriggered.class, this::logIgnoredSensor)
                .match(SmartHomeProtocol.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Valid PIN: arming cancelled.");
                    getTimers().cancel(DELAY_TIMER_KEY);
                    goTo("DISARMED", disarmedState());
                }));
    }

    // Armed state:
    // - SensorTriggered in an active zone: -> Entry delay
    // - ValidPinEntered: -> Disarmed
    private Receive armedState() {
        return withCommonMessages(receiveBuilder()
                .match(SmartHomeProtocol.SensorTriggered.class, this::onSensorTriggeredArmed)
                .match(SmartHomeProtocol.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Valid PIN: disarming.");
                    goTo("DISARMED", disarmedState());
                }));
    }

    // Entry delay state:
    // - ValidPinEntered: -> Disarmed
    // - EntryDelayTimeout: -> Alarm
    // - SensorTriggered: no additional effect
    private Receive entryDelayState() {
        return withCommonMessages(receiveBuilder()
                .match(SmartHomeProtocol.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Valid PIN entered in time: disarming.");
                    getTimers().cancel(DELAY_TIMER_KEY);
                    goTo("DISARMED", disarmedState());
                })
                .match(EntryDelayTimeout.class, m -> {
                    System.out.println("[ControlUnit] Entry delay expired: ALARM!");
                    tellSirens(new SmartHomeProtocol.ActivateSiren());
                    goTo("ALARM", alarmState());
                })
                .match(SmartHomeProtocol.SensorTriggered.class, this::logIgnoredSensor));
    }

    // Alarm state:
    // - ValidPinEntered: sirens off, -> Disarmed
    // - SensorTriggered: already triggered
    private Receive alarmState() {
        return withCommonMessages(receiveBuilder()
                .match(SmartHomeProtocol.ValidPinEntered.class, m -> {
                    System.out.println("[ControlUnit] Valid PIN: stopping the alarm.");
                    tellSirens(new SmartHomeProtocol.DeactivateSiren());
                    goTo("DISARMED", disarmedState());
                })
                .match(SmartHomeProtocol.SensorTriggered.class, this::logIgnoredSensor));
    }

    // Messages handled the same way in every state. Anything not handled by the
    // state is logged and ignored (for example a wrong PIN, or an arming request
    // while already armed).
    private Receive withCommonMessages(ReceiveBuilder builder) {
        return builder
                .match(SmartHomeProtocol.RegisterSiren.class, this::onRegisterSiren)
                .match(Terminated.class, this::onSirenTerminated)
                .match(SmartHomeProtocol.GetState.class, m ->
                        m.replyTo().tell(new SmartHomeProtocol.CurrentState(stateName), getSelf()))
                .match(SmartHomeProtocol.SimulateCrash.class, m -> {
                    throw new RuntimeException("Simulated crash: " + m.reason());
                })
                .match(SmartHomeProtocol.InvalidPinEntered.class, m ->
                        System.out.println("[ControlUnit] WARNING! Invalid PIN entered on " + m.keypadId()))
                .matchAny(m -> System.out.println("[ControlUnit] " + stateName + ": ignored " + m))
                .build();
    }

    private void onArmSystemRequest(SmartHomeProtocol.ArmSystemRequest request) {
        activeZones.clear();
        activeZones.addAll(request.zonesToArm());
        fullArming = activeZones.isEmpty();
        System.out.println("[ControlUnit] Arming " + (fullArming ? "all zones" : activeZones)
                + ". Exit delay of " + exitDelay.toSeconds() + "s started.");
        getTimers().startSingleTimer(DELAY_TIMER_KEY, new ExitDelayTimeout(), exitDelay);
        goTo("EXIT_DELAY", exitDelayState());
    }

    private void onSensorTriggeredArmed(SmartHomeProtocol.SensorTriggered event) {
        if (fullArming || activeZones.contains(event.zone())) {
            System.out.println("[ControlUnit] WARNING! " + event.type() + " sensor " + event.sensorId()
                    + " in active zone " + event.zone() + ". Entry delay of " + entryDelay.toSeconds() + "s started.");
            getTimers().startSingleTimer(DELAY_TIMER_KEY, new EntryDelayTimeout(), entryDelay);
            goTo("ENTRY_DELAY", entryDelayState());
        } else {
            System.out.println("[ControlUnit] Ignored sensor " + event.sensorId() + " (zone " + event.zone() + " is inactive).");
        }
    }

    private void logIgnoredSensor(SmartHomeProtocol.SensorTriggered event) {
        System.out.println("[ControlUnit] " + stateName + ": logged " + event.type() + " event from " + event.sensorId());
    }

    private void onRegisterSiren(SmartHomeProtocol.RegisterSiren register) {
        if (sirens.add(register.siren())) {
            System.out.println("[ControlUnit] Siren registered: " + register.siren().path());
            getContext().watch(register.siren()); // we get Terminated if it (or its node) goes away
            if (stateName.equals("ALARM")) {
                register.siren().tell(new SmartHomeProtocol.ActivateSiren(), getSelf());
            }
        }
    }

    private void onSirenTerminated(Terminated terminated) {
        sirens.remove(terminated.getActor());
        System.out.println("[ControlUnit] Siren unreachable, removed: " + terminated.getActor().path());
    }

    private void tellSirens(Object message) {
        if (sirens.isEmpty()) {
            System.out.println("[ControlUnit] WARNING! No siren registered.");
        }
        for (ActorRef siren : sirens) {
            siren.tell(message, getSelf());
        }
    }

    private void goTo(String newState, Receive behavior) {
        System.out.println("[ControlUnit] " + stateName + " -> " + newState);
        stateName = newState;
        getContext().become(behavior);
    }
}
