package kafka_LegId.broker.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiversionEvent {

    private FlightInfo flightInfo;
    private String flightStatus;
    private ContinuationLeg continuationLeg;
    private String effectiveArrivalStation;
    private String diversionCode;
    private OffsetDateTime estimatedInBlock;
    private String registration;
    private OffsetDateTime eventReceived;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContinuationLeg {

        private String startStation;
        private String endStation;
        private OffsetDateTime scheduledStartTime;
        private OffsetDateTime scheduledEndTime;

    }

}
