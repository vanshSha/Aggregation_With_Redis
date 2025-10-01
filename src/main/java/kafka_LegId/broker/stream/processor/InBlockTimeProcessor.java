package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightInfo;
import kafka_LegId.broker.event.InBlockTimeEvent;
import kafka_LegId.broker.stream.InBlockTimeStream;
import org.springframework.stereotype.Component;

@Component
public class InBlockTimeProcessor {

    public InBlockTimeEvent inBlockTimeEvent(InBlockTimeEvent inComingEvent, InBlockTimeEvent existingEvent) {

        if (inComingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {

            InBlockTimeEvent newEvent = new InBlockTimeEvent();

          //  if(inComingEvent.getFlightInfo()!= null && inComingEvent.getFlightInfo().getLegId() != null) {
                FlightInfo flightInfo = new FlightInfo();
                flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
                newEvent.setFlightInfo(flightInfo);

          //  }
            newEvent.setCurrentInBlock(inComingEvent.getCurrentInBlock());
            newEvent.setPreviousInBlock(inComingEvent.getPreviousInBlock());
            newEvent.setTimeType(inComingEvent.getTimeType());
            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;

        }

        if (
                 existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())) {

            existingEvent.setCurrentInBlock(inComingEvent.getCurrentInBlock());
            existingEvent.setPreviousInBlock(inComingEvent.getPreviousInBlock());
            existingEvent.setTimeType(inComingEvent.getTimeType());
            existingEvent.setEventReceived(inComingEvent.getEventReceived());
        }
        return existingEvent;
    }
}
