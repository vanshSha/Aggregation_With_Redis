package kafka_LegId.broker.stream.processor;

import kafka_LegId.broker.event.EquipmentEvent;
import kafka_LegId.broker.event.FlightInfo;
import org.springframework.stereotype.Component;

@Component
public class EquipmentProcessor {

    public EquipmentEvent equipmentProcessor(EquipmentEvent existingEvent, EquipmentEvent inComingEvent) {

        // If incoming event is null, just return existing (nothing to update)
        if (inComingEvent == null) {
            return existingEvent;  // first record → create safely
        }


        if (existingEvent == null) {
            EquipmentEvent newEvent = new EquipmentEvent();

            FlightInfo flightInfo = new FlightInfo();
            flightInfo.setLegId(inComingEvent.getFlightInfo().getLegId());
            newEvent.setFlightInfo(flightInfo);


            EquipmentEvent.CurrentAssignment current = new EquipmentEvent.CurrentAssignment();
            current.setPlannedAircraftType(inComingEvent.getCurrent().getPlannedAircraftType());
            current.setAircraftConfiguration(inComingEvent.getCurrent().getAircraftConfiguration());
            current.setAssignedAircraftConfiguration(inComingEvent.getCurrent().getAssignedAircraftConfiguration());
            newEvent.setCurrent(current);

            EquipmentEvent.Aircraft aircraft = new EquipmentEvent.Aircraft();
            aircraft.setRegistration(inComingEvent.getCurrent().getAircraft().getRegistration());
            aircraft.setType(inComingEvent.getCurrent().getAircraft().getType());
            current.setAircraft(aircraft);

            // previous
            EquipmentEvent.PreviousAssignment previous = new EquipmentEvent.PreviousAssignment();
            previous.setPlannedAircraftType(inComingEvent.getPrevious().getPlannedAircraftType());
            previous.setAircraftConfiguration(inComingEvent.getPrevious().getAircraftConfiguration());
            previous.setAssignedAircraftConfiguration(inComingEvent.getPrevious().getAssignedAircraftConfiguration());
            newEvent.setPrevious(previous);

            EquipmentEvent.Aircraft aircraftP = new EquipmentEvent.Aircraft();
            aircraftP.setRegistration(inComingEvent.getPrevious().getAircraft().getRegistration());
            aircraftP.setType(inComingEvent.getPrevious().getAircraft().getType());
            previous.setAircraft(aircraftP);

            newEvent.setEventReceived(inComingEvent.getEventReceived());

            return newEvent;
        }

        if (existingEvent != null
                && existingEvent.getFlightInfo() != null
                && inComingEvent.getFlightInfo() != null
                && existingEvent.getFlightInfo().getLegId().equals(inComingEvent.getFlightInfo().getLegId())) {

            existingEvent.getCurrent().setPlannedAircraftType(inComingEvent.getCurrent().getPlannedAircraftType());
            existingEvent.getCurrent().getAircraft().setRegistration(inComingEvent.getCurrent().getAircraft().getRegistration());
            existingEvent.getCurrent().getAircraft().setType(inComingEvent.getCurrent().getAircraft().getType());
            existingEvent.getCurrent().setAircraftConfiguration(inComingEvent.getCurrent().getAircraftConfiguration());
            existingEvent.getCurrent().setAssignedAircraftConfiguration(inComingEvent.getCurrent().getAssignedAircraftConfiguration());

            existingEvent.getPrevious().setPlannedAircraftType(inComingEvent.getPrevious().getPlannedAircraftType());
            existingEvent.getPrevious().getAircraft().setRegistration(inComingEvent.getPrevious().getAircraft().getRegistration());
            existingEvent.getPrevious().getAircraft().setType(inComingEvent.getPrevious().getAircraft().getType());
            existingEvent.getPrevious().setAircraftConfiguration(inComingEvent.getPrevious().getAircraftConfiguration());
            existingEvent.getPrevious().setAssignedAircraftConfiguration(inComingEvent.getPrevious().getAssignedAircraftConfiguration());

            existingEvent.setEventReceived(inComingEvent.getEventReceived());
        }


        return existingEvent;
    }
}
