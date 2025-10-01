package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EquipmentEvent {

    private FlightInfo flightInfo;
    private CurrentAssignment current;
    private PreviousAssignment previous;
    private OffsetDateTime eventReceived;

    @Data
    public static class CurrentAssignment {

        private String plannedAircraftType;
        private Aircraft aircraft;
        private String aircraftConfiguration;
        private String assignedAircraftConfiguration;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PreviousAssignment {

        private String plannedAircraftType;
        private Aircraft aircraft;
        private String aircraftConfiguration;
        private String assignedAircraftConfiguration;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Aircraft {

        private String registration;
        private String type;

    }
}
