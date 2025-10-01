package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.DelayEvent;
import kafka_LegId.broker.event.DiversionEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.apache.kafka.common.metrics.stats.Histogram;
import org.springframework.stereotype.Component;

@Component
public class DiversionEventProcessor {

    public DiversionEvent diversionEventProcessor(DiversionEvent incomingEvent, DiversionEvent existingEvent) {

        if (incomingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {

            DiversionEvent newEvent = new DiversionEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(incomingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);

            newEvent.setFlightStatus(incomingEvent.getFlightStatus());

            DiversionEvent.ContinuationLeg continuationLeg = new DiversionEvent.ContinuationLeg();
            continuationLeg.setStartStation(incomingEvent.getContinuationLeg().getStartStation());
            continuationLeg.setEndStation(incomingEvent.getContinuationLeg().getEndStation());
            continuationLeg.setScheduledStartTime(incomingEvent.getContinuationLeg().getScheduledStartTime());
            continuationLeg.setScheduledEndTime(incomingEvent.getContinuationLeg().getScheduledEndTime());

            newEvent.setContinuationLeg(continuationLeg);

            newEvent.setEffectiveArrivalStation(incomingEvent.getEffectiveArrivalStation());
            newEvent.setDiversionCode(incomingEvent.getDiversionCode());
            newEvent.setEstimatedInBlock(incomingEvent.getEstimatedInBlock());
            newEvent.setRegistration(incomingEvent.getRegistration());
            newEvent.setEventReceived(incomingEvent.getEventReceived());

            return newEvent;
        }

        if (existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())) {


            existingEvent.setFlightStatus(incomingEvent.getFlightStatus());

            DiversionEvent.ContinuationLeg newDiversionEvent = new DiversionEvent.ContinuationLeg();
            newDiversionEvent.setStartStation(incomingEvent.getContinuationLeg().getStartStation());
            newDiversionEvent.setEndStation(incomingEvent.getContinuationLeg().getEndStation());
            newDiversionEvent.setScheduledStartTime(incomingEvent.getContinuationLeg().getScheduledStartTime());
            newDiversionEvent.setScheduledEndTime(incomingEvent.getContinuationLeg().getScheduledEndTime());

            existingEvent.setContinuationLeg(newDiversionEvent);

            existingEvent.setEffectiveArrivalStation(incomingEvent.getEffectiveArrivalStation());
            existingEvent.setDiversionCode(incomingEvent.getDiversionCode());
            existingEvent.setEstimatedInBlock(incomingEvent.getEstimatedInBlock());
            existingEvent.setRegistration(incomingEvent.getRegistration());
            existingEvent.setEventReceived(incomingEvent.getEventReceived());

        }
        return existingEvent;

    }
}
