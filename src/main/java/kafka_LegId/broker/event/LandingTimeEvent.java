package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LandingTimeEvent {

    private FlightInfo flightInfo;
    private OffsetDateTime currentLanding;
    private OffsetDateTime previousLanding;
    private String timeType;
    private OffsetDateTime eventReceived;

}