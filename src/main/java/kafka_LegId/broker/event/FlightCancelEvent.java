package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlightCancelEvent {

    private FlightInfo flightInfo;
    private Current currentCancelDetails;
    private Previous PreviousCancelDetails;
    private OffsetDateTime eventReceived;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Current {

        private String operationalStatus;
        private String cancellationCode;
        private String serviceType;
        private String flightStatus;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Previous {

        private String operationalStatus;
        private String cancellationCode;
        private String serviceType;
        private String flightStatus;

    }


}
