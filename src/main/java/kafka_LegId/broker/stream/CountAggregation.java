package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class CountAggregation {

    private FlightInfo flightInfo;
    private DelayEvent delayEvent;
    private DiversionEvent diversionEvent;
    private DoorCloseEvent doorClose;
    private EquipmentEvent equipmentEvent;
    private FlightCancelEvent flightCancelEvent;
    private FlightDeleteEvent flightDelete;
    private FlightReturnEvent flightReturnEvent;
    private GateChangeEvent gateChangeEvent;
    private InBlockTimeEvent inBlockTimeEvent;
    private LandingTimeEvent landingTimeEvent;
    private OffBlockTimeEvent offBlockTimeEvent;
    private TakeOffTimeEvent takeOffTimeEvent;














    /*  this stuff regarding to count
    private int doorCloseCount;
    private int flightDeleteCount;

    public CountAggregation() {
        this.doorCloseCount = 0;
        this.flightDeleteCount = 0;
    }
    public void incrementDoorCloseCount() {
        this.doorCloseCount++;
    }

    public void incrementFlightDeleteCount() {
        this.flightDeleteCount++;
    }
*/
}
