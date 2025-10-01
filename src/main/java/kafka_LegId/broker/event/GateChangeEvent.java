package kafka_LegId.broker.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

// TerminalEvent
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true) // ignores unknown fields like LEGID
public class GateChangeEvent {

    private FlightInfo flightInfo;
    private Current current;
    private Previous previous;
    private OffsetDateTime eventReceived;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Current {
        private String startGate;
        private String endGate;
        private String startTerminal;
        private String endTerminal;
        private String startStand;
        private String endStand;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Previous {
        private String startGate;
        private String endGate;
        private String startTerminal;
        private String endTerminal;
        private String startStand;
        private String endStand;
    }


}
