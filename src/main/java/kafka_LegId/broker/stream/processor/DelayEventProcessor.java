package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.DelayEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class DelayEventProcessor {

    public DelayEvent delayEventProcessing(DelayEvent incomingEvent, DelayEvent existingEvent) {

        if (incomingEvent == null) {
            return existingEvent;
        }

        if (existingEvent == null) {

            DelayEvent newEvent = new DelayEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(incomingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);


            DelayEvent.Delays delays = new DelayEvent.Delays();
            delays.setDelayLimit(incomingEvent.getDelays().getDelayLimit());
            delays.setRemark(incomingEvent.getDelays().getRemark());
            delays.setTotal(incomingEvent.getDelays().getTotal());
            newEvent.setDelays(delays);


            List<DelayEvent.Delay> delayList = new ArrayList<>();
            for (DelayEvent.Delay d : incomingEvent.getDelays().getDelay()) {
                DelayEvent.Delay delayItem = new DelayEvent.Delay();
                delayItem.setTime(d.getTime());
                delayItem.setReason(d.getReason());
                delayItem.setDelayNumber(d.getDelayNumber());
                delayItem.setRootCause(d.isRootCause());
                delayItem.setRemark(d.getRemark());
                delayList.add(delayItem);

            }
            delays.setDelay(delayList);

            newEvent.setCurrentOffBlock(incomingEvent.getCurrentOffBlock());
            newEvent.setPreviousOffBlock(incomingEvent.getPreviousOffBlock());
            newEvent.setTimeType(incomingEvent.getTimeType());
            newEvent.setEventReceived(incomingEvent.getEventReceived());

            return newEvent;

        } else {
            if (
//                    existingEvent != null
//                            && incomingEvent != null
//                            &&
                    existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())) {

                existingEvent.setCurrentOffBlock(incomingEvent.getCurrentOffBlock());
                existingEvent.setPreviousOffBlock(incomingEvent.getPreviousOffBlock());
                existingEvent.setTimeType(incomingEvent.getTimeType());
                existingEvent.setEventReceived(incomingEvent.getEventReceived());
                existingEvent.getDelays().setDelayLimit(incomingEvent.getDelays().getDelayLimit());
                existingEvent.getDelays().setRemark(incomingEvent.getDelays().getRemark());
                existingEvent.getDelays().setTotal(incomingEvent.getDelays().getTotal());


                if (incomingEvent.getDelays().getDelay() != null) {

                    List<DelayEvent.Delay> delayList = new ArrayList<>();
                    for (DelayEvent.Delay d : incomingEvent.getDelays().getDelay()) {
                        DelayEvent.Delay delayItem = new DelayEvent.Delay();
                        delayItem.setTime(d.getTime());
                        delayItem.setReason(d.getReason());
                        delayItem.setDelayNumber(d.getDelayNumber());
                        delayItem.setRootCause(d.isRootCause());
                        delayItem.setRemark(d.getRemark());
                        delayList.add(delayItem);
                    }
                    existingEvent.getDelays().setDelay(delayList);
                }

            }

            return existingEvent;

        }
    }
}
