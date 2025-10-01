package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OffBlockTimeEvent {

    private FlightInfo flightInfo;
    private OffsetDateTime currentOffBlock;
    private OffsetDateTime previousOffBlock;
    private String timeType;
    private OffsetDateTime eventReceived;

}
