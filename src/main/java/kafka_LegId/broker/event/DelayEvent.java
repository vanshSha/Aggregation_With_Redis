package kafka_LegId.broker.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DelayEvent {

    private FlightInfo flightInfo;
    private Delays delays;
    private Delay delay;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") // Ensure Jackson reads ISO date string
    private LocalDateTime currentOffBlock;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime previousOffBlock;

    private String timeType;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime eventReceived;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Delays {
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        private LocalDateTime delayLimit;

        private String remark;
        private List<Delay> delay;
        private String total;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Delay {
        private String time;
        private String reason;
        private int delayNumber;

        @JsonProperty("rootCause") // Map JSON "isRootCause" → rootCause field
        private boolean rootCause;

        private String remark;
    }
}
