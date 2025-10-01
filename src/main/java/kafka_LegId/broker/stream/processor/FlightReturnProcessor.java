package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightInfo;
import kafka_LegId.broker.event.FlightReturnEvent;
import org.springframework.stereotype.Component;

@Component
public class FlightReturnProcessor {


    public FlightReturnEvent flightReturnEvent(FlightReturnEvent incomingEvent, FlightReturnEvent existingEvent) {

        if (incomingEvent == null) {
            return existingEvent;
        }

        // Case 2: no existing → create new
        if (existingEvent == null) {
            FlightReturnEvent newEvent = new FlightReturnEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(incomingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);

            FlightReturnEvent.ReturnAtom newReturnAtom = new FlightReturnEvent.ReturnAtom();
            newReturnAtom.setReturnNumber(incomingEvent.getReturnAtom().getReturnNumber());

            // EstimatedTimes
            FlightReturnEvent.EstimatedTimes est = new FlightReturnEvent.EstimatedTimes();
            est.setOffBlock(incomingEvent.getReturnAtom().getEstimatedTimes().getOffBlock());
            est.setInBlock(incomingEvent.getReturnAtom().getEstimatedTimes().getInBlock());
            est.setTakeoffTime(incomingEvent.getReturnAtom().getEstimatedTimes().getTakeoffTime());
            est.setLandingTime(incomingEvent.getReturnAtom().getEstimatedTimes().getLandingTime());
            newReturnAtom.setEstimatedTimes(est);

            // ActualTimes
            FlightReturnEvent.ActualTimes act = new FlightReturnEvent.ActualTimes();
            act.setOffBlock(incomingEvent.getReturnAtom().getActualTimes().getOffBlock());
            act.setInBlock(incomingEvent.getReturnAtom().getActualTimes().getInBlock());
            act.setTakeoffTime(incomingEvent.getReturnAtom().getActualTimes().getTakeoffTime());
            act.setLandingTime(incomingEvent.getReturnAtom().getActualTimes().getLandingTime());
            act.setDoorClose(incomingEvent.getReturnAtom().getActualTimes().getDoorClose());
            newReturnAtom.setActualTimes(act);

            newReturnAtom.setAircraft(incomingEvent.getReturnAtom().getAircraft());
            newReturnAtom.setScheduleStatus(incomingEvent.getReturnAtom().getScheduleStatus());
            newReturnAtom.setInBlockFuel(incomingEvent.getReturnAtom().getInBlockFuel());
            newReturnAtom.setUnit(incomingEvent.getReturnAtom().getUnit());

            newEvent.setReturnAtom(newReturnAtom); // attach returnAtom
            newEvent.setEventReceived(incomingEvent.getEventReceived());
            return newEvent;
        }

        // Case 3: update existing
        if (existingEvent.getFlightInfo() != null
                && incomingEvent.getFlightInfo() != null
                && existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())) {

            existingEvent.getReturnAtom().setReturnNumber(incomingEvent.getReturnAtom().getReturnNumber());

            existingEvent.getReturnAtom().getEstimatedTimes().setOffBlock(incomingEvent.getReturnAtom().getEstimatedTimes().getOffBlock());
            existingEvent.getReturnAtom().getEstimatedTimes().setInBlock(incomingEvent.getReturnAtom().getEstimatedTimes().getInBlock());
            existingEvent.getReturnAtom().getEstimatedTimes().setTakeoffTime(incomingEvent.getReturnAtom().getEstimatedTimes().getTakeoffTime());
            existingEvent.getReturnAtom().getEstimatedTimes().setLandingTime(incomingEvent.getReturnAtom().getEstimatedTimes().getLandingTime());

            existingEvent.getReturnAtom().getActualTimes().setOffBlock(incomingEvent.getReturnAtom().getActualTimes().getOffBlock());
            existingEvent.getReturnAtom().getActualTimes().setInBlock(incomingEvent.getReturnAtom().getActualTimes().getInBlock());
            existingEvent.getReturnAtom().getActualTimes().setTakeoffTime(incomingEvent.getReturnAtom().getActualTimes().getTakeoffTime());
            existingEvent.getReturnAtom().getActualTimes().setLandingTime(incomingEvent.getReturnAtom().getActualTimes().getLandingTime());
            existingEvent.getReturnAtom().getActualTimes().setDoorClose(incomingEvent.getReturnAtom().getActualTimes().getDoorClose());

            existingEvent.getReturnAtom().setAircraft(incomingEvent.getReturnAtom().getAircraft());
            existingEvent.getReturnAtom().setScheduleStatus(incomingEvent.getReturnAtom().getScheduleStatus());
            existingEvent.getReturnAtom().setInBlockFuel(incomingEvent.getReturnAtom().getInBlockFuel());
            existingEvent.getReturnAtom().setUnit(incomingEvent.getReturnAtom().getUnit());

            existingEvent.setEventReceived(incomingEvent.getEventReceived());
        }

        return existingEvent;
    }

}
