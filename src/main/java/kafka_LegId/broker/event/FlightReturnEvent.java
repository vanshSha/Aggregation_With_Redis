package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlightReturnEvent {

    private FlightInfo flightInfo;
    private ReturnAtom returnAtom;
    private OffsetDateTime eventReceived;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReturnAtom {
        private int returnNumber;
        private EstimatedTimes estimatedTimes;
        private ActualTimes actualTimes;
        private String aircraft;
        private String scheduleStatus;
        private String inBlockFuel;
        private String unit;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EstimatedTimes {
        private String offBlock;
        private String inBlock;
        private String takeoffTime;
        private String landingTime;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ActualTimes {
        private String offBlock;
        private String inBlock;
        private String takeoffTime;
        private String landingTime;
        private String doorClose;
    }

}


