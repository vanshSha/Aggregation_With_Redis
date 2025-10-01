package kafka_LegId.broker.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true) //ignore any extra JSON fields not defined in your class, preventing deserialization errors
public class DoorCloseEvent {

    private FlightInfo flightInfo;
    private OffsetDateTime currentOffBlock;
    private OffsetDateTime previousOffBlock;
    private String timeType;
    private OffsetDateTime eventReceived;



}
