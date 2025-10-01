package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightInfo;
import kafka_LegId.broker.event.GateChangeEvent;
import org.springframework.stereotype.Component;

@Component
public class GateChangeEventProcessor {

    public GateChangeEvent gateChangeEventProcessor(GateChangeEvent inComingEvent, GateChangeEvent existingEvent) {

        // Case 1: no incoming → keep existing
        if (inComingEvent == null) {
            return existingEvent;
        }

        if(existingEvent == null){

            GateChangeEvent newEvent = new GateChangeEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);

            GateChangeEvent.Current current = new GateChangeEvent.Current();
            current.setStartGate(inComingEvent.getCurrent().getStartGate());
            current.setEndGate(inComingEvent.getCurrent().getEndGate());
            current.setStartTerminal(inComingEvent.getCurrent().getStartTerminal());
            current.setEndTerminal(inComingEvent.getCurrent().getEndTerminal());
            current.setStartStand(inComingEvent.getCurrent().getStartStand());
            current.setEndStand(inComingEvent.getCurrent().getEndStand());
            newEvent.setCurrent(current);

            // Map previous assignment
            GateChangeEvent.Previous previous = new GateChangeEvent.Previous();
            previous.setStartGate(inComingEvent.getPrevious().getStartGate());
            previous.setEndGate(inComingEvent.getPrevious().getEndGate());
            previous.setStartTerminal(inComingEvent.getPrevious().getStartTerminal());
            previous.setEndTerminal(inComingEvent.getPrevious().getEndTerminal());
            previous.setStartStand(inComingEvent.getPrevious().getStartStand());
            previous.setEndStand(inComingEvent.getPrevious().getEndStand());
            newEvent.setPrevious(previous);

            newEvent.setEventReceived(inComingEvent.getEventReceived());
            return newEvent;
        }
        if(existingEvent.getFlightInfo() != null
                && inComingEvent.getFlightInfo() != null
                && existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())) {

            existingEvent.getCurrent().setStartGate(inComingEvent.getCurrent().getStartGate());
            existingEvent.getCurrent().setEndGate(inComingEvent.getCurrent().getEndGate());
            existingEvent.getCurrent().setStartTerminal(inComingEvent.getCurrent().getStartTerminal());
            existingEvent.getCurrent().setEndTerminal(inComingEvent.getCurrent().getEndTerminal());
            existingEvent.getCurrent().setStartStand(inComingEvent.getCurrent().getStartStand());
            existingEvent.getCurrent().setEndStand(inComingEvent.getCurrent().getEndStand());

            existingEvent.getPrevious().setStartGate(inComingEvent.getPrevious().getStartGate());
            existingEvent.getPrevious().setEndGate(inComingEvent.getPrevious().getEndGate());
            existingEvent.getPrevious().setStartTerminal(inComingEvent.getPrevious().getStartTerminal());
            existingEvent.getPrevious().setEndTerminal(inComingEvent.getPrevious().getEndTerminal());
            existingEvent.getPrevious().setStartStand(inComingEvent.getPrevious().getStartStand());
            existingEvent.getPrevious().setEndStand(inComingEvent.getPrevious().getEndStand());

            existingEvent.setEventReceived(inComingEvent.getEventReceived());



        }

        return existingEvent;
    }
}
