package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlightDeleteEvent {

    private FlightInfo flightInfo;
    private String operationalStatus;
    private String eventReceived;

}
