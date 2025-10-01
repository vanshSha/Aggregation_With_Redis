package kafka_LegId.broker.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AggregationEvent {

    private FlightInfo flightInfo;
    private DelayEvent delayEvent;
    private DiversionEvent diversionEvent;
    private DoorCloseEvent doorCloseEvent;
    private EquipmentEvent equipmentEvent;
    private FlightCancelEvent flightCancelEvent;
    private FlightDeleteEvent flightDelete;
    private FlightReturnEvent flightReturnEvent;
    private GateChangeEvent gateChangeEvent;
    private InBlockTimeEvent inBlockTimeEvent;
    private LandingTimeEvent landingTimeEvent;
    private OffBlockTimeEvent offBlockTimeEvent;
    private TakeOffTimeEvent takeOffTimeEvent;


//    // Proper custom constructor
//    public AggregationEvent(FlightInfo flightInfo, DoorCloseEvent d, FlightDeleteEvent f) {
//        this.flightInfo = flightInfo;
//        this.doorClose = d;
//        this.flightDelete = f;
//    }
}
