package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.EquipmentEvent;
import kafka_LegId.broker.event.FlightCancelEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.springframework.stereotype.Component;

@Component
public class FlightCancelProcessor {

    public FlightCancelEvent flightCancelEvent(FlightCancelEvent inComingEvent, FlightCancelEvent existingEvent) {

        // If incoming event is null, just return existing (nothing to update)
        if (inComingEvent == null) {
            return existingEvent;  // first record → create safely
        }

        if (existingEvent == null) {
            FlightCancelEvent newEvent = new FlightCancelEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);


            FlightCancelEvent.Current current = new FlightCancelEvent.Current();
            current.setOperationalStatus(inComingEvent.getCurrentCancelDetails().getOperationalStatus());
            current.setCancellationCode(inComingEvent.getCurrentCancelDetails().getCancellationCode());
            current.setServiceType(inComingEvent.getCurrentCancelDetails().getServiceType());
            current.setFlightStatus(inComingEvent.getCurrentCancelDetails().getFlightStatus());
            newEvent.setCurrentCancelDetails(current);


            FlightCancelEvent.Previous previous = new FlightCancelEvent.Previous();
            previous.setOperationalStatus(inComingEvent.getCurrentCancelDetails().getOperationalStatus());
            previous.setCancellationCode(inComingEvent.getCurrentCancelDetails().getCancellationCode());
            previous.setServiceType(inComingEvent.getCurrentCancelDetails().getServiceType());
            previous.setFlightStatus(inComingEvent.getCurrentCancelDetails().getFlightStatus());
            newEvent.setPreviousCancelDetails(previous);

            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;
        }
        if (existingEvent != null
                && existingEvent.getFlightInfo() != null
                && inComingEvent.getFlightInfo() != null
                && existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())) {

            existingEvent.getCurrentCancelDetails().setOperationalStatus(inComingEvent.getCurrentCancelDetails().getOperationalStatus());
            existingEvent.getCurrentCancelDetails().setCancellationCode(inComingEvent.getCurrentCancelDetails().getCancellationCode());
            existingEvent.getCurrentCancelDetails().setServiceType(inComingEvent.getCurrentCancelDetails().getServiceType());
            existingEvent.getCurrentCancelDetails().setFlightStatus(inComingEvent.getCurrentCancelDetails().getFlightStatus());


            existingEvent.getPreviousCancelDetails().setOperationalStatus(inComingEvent.getPreviousCancelDetails().getOperationalStatus());
            existingEvent.getPreviousCancelDetails().setCancellationCode(inComingEvent.getPreviousCancelDetails().getCancellationCode());
            existingEvent.getPreviousCancelDetails().setServiceType(inComingEvent.getPreviousCancelDetails().getServiceType());
            existingEvent.getPreviousCancelDetails().setFlightStatus(inComingEvent.getPreviousCancelDetails().getFlightStatus());

            existingEvent.setEventReceived(inComingEvent.getEventReceived());
        }

        return existingEvent;
    }
}
