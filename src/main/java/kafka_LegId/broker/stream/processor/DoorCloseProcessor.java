package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.DoorCloseEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.springframework.stereotype.Component;

@Component
public class DoorCloseProcessor {

    public DoorCloseEvent doorCloseProcessor(DoorCloseEvent incomingEvent, DoorCloseEvent existingEvent) {

        if (incomingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {
            DoorCloseEvent newEvent = new DoorCloseEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(incomingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);

            newEvent.setCurrentOffBlock(incomingEvent.getCurrentOffBlock());
            newEvent.setPreviousOffBlock(incomingEvent.getPreviousOffBlock());
            newEvent.setTimeType(incomingEvent.getTimeType());
            newEvent.setEventReceived(incomingEvent.getEventReceived());

            return newEvent;
        }

        if (existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())) {

            existingEvent.setCurrentOffBlock(incomingEvent.getCurrentOffBlock());
            existingEvent.setPreviousOffBlock(incomingEvent.getPreviousOffBlock());
            existingEvent.setTimeType(incomingEvent.getTimeType());
            existingEvent.setEventReceived(incomingEvent.getEventReceived());
        }

        return existingEvent;

    }
}
