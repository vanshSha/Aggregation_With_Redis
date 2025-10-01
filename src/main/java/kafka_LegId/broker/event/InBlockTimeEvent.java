package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InBlockTimeEvent {

    private FlightInfo flightInfo;
    private OffsetDateTime currentInBlock;
    private OffsetDateTime previousInBlock;
    private String timeType;
    private OffsetDateTime eventReceived;


}


