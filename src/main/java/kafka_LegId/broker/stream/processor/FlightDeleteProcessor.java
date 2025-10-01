package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightDeleteEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.springframework.stereotype.Component;

@Component
public class FlightDeleteProcessor {

    public FlightDeleteEvent flightDeleteProcessorEvent(FlightDeleteEvent inComingEvent, FlightDeleteEvent existingEvent) {

        if (inComingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {

            FlightDeleteEvent newEvent = new FlightDeleteEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);

            newEvent.setOperationalStatus(inComingEvent.getOperationalStatus());
            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;
        } else {

            if (
//                    existingEvent != null
//                    && existingEvent.getFlightInfo() != null
//                    && inComingEvent.getFlightInfo() != null
//                    &&
                    existingEvent.getFlightInfo().getLegId()
                            .equals(inComingEvent.getFlightInfo().getLegId())) {

                existingEvent.setOperationalStatus(inComingEvent.getOperationalStatus());
                existingEvent.setEventReceived(inComingEvent.getEventReceived());

            }
        }

        return existingEvent;
    }
}
