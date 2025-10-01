package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightInfo;
import kafka_LegId.broker.event.OffBlockTimeEvent;
import kafka_LegId.broker.stream.OffBlockTimeStream;
import org.springframework.stereotype.Component;

@Component
public class OffBlockTimeProcessor {

    public OffBlockTimeEvent offBlockTimeEvent(OffBlockTimeEvent inComingEvent, OffBlockTimeEvent existingEvent) {

        if (inComingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {

            OffBlockTimeEvent newEvent = new OffBlockTimeEvent();

            if (inComingEvent.getFlightInfo() != null) {
                FlightInfo flightInfo = new FlightInfo();
                flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
                newEvent.setFlightInfo(flightInfo);
            }

            newEvent.setCurrentOffBlock(inComingEvent.getCurrentOffBlock());
            newEvent.setPreviousOffBlock(inComingEvent.getPreviousOffBlock());
            newEvent.setTimeType(inComingEvent.getTimeType());
            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;
        }

        if (existingEvent.getFlightInfo() != null
                && inComingEvent.getFlightInfo() != null
                && existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())) {

            existingEvent.setCurrentOffBlock(inComingEvent.getCurrentOffBlock());
            existingEvent.setPreviousOffBlock(inComingEvent.getPreviousOffBlock());
            existingEvent.setTimeType(inComingEvent.getTimeType());
            existingEvent.setEventReceived(inComingEvent.getEventReceived());


        }
        return existingEvent;
    }
}
