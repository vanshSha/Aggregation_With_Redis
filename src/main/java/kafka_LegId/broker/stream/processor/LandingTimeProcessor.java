package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.FlightInfo;
import kafka_LegId.broker.event.LandingTimeEvent;
import kafka_LegId.broker.stream.LandingTimeStream;
import org.springframework.stereotype.Component;

@Component
public class LandingTimeProcessor {

    public LandingTimeEvent landingTimeEvent(LandingTimeEvent inComingEvent, LandingTimeEvent existingEvent) {

        if (inComingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {
            LandingTimeEvent newEvent = new LandingTimeEvent();

           // if (inComingEvent.getFlightInfo() != null) {
                FlightInfo flightInfo = new FlightInfo();
                flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
                newEvent.setFlightInfo(flightInfo);
           // }
            newEvent.setCurrentLanding(inComingEvent.getCurrentLanding());
            newEvent.setPreviousLanding(inComingEvent.getPreviousLanding());
            newEvent.setTimeType(inComingEvent.getTimeType());
            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;
        }

        if(existingEvent.getFlightInfo() != null
          && inComingEvent.getFlightInfo() != null
        && existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())){

            existingEvent.setCurrentLanding(inComingEvent.getCurrentLanding());
            existingEvent.setPreviousLanding(inComingEvent.getPreviousLanding());
            existingEvent.setTimeType(inComingEvent.getTimeType());
            existingEvent.setEventReceived(inComingEvent.getEventReceived());
        }

        return existingEvent;
    }
}
