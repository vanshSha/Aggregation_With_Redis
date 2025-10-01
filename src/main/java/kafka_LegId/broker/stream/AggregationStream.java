package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import kafka_LegId.broker.event.*;
import kafka_LegId.broker.stream.processor.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class AggregationStream {

    @Autowired
    private ObjectMapper objectMapper;

    // Redis template
    private final RedisTemplate<String, String> redisTemplate;

    public AggregationStream(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // custom logic processor
    @Autowired
    private DelayEventProcessor delayProcessor;

    @Autowired
    private DiversionEventProcessor diversionProcessor;

    @Autowired
    private DoorCloseProcessor doorProcessor;

    @Autowired
    private EquipmentProcessor equipmentProcessor;

    @Autowired
    private FlightCancelProcessor flightCancelProcessor;

    @Autowired
    private FlightDeleteProcessor flightDeleteProcessor;

    @Autowired
    private FlightReturnProcessor flightReturnProcessor;

    @Autowired
    private GateChangeEventProcessor gateChangeEventProcessor;

    @Autowired
    private InBlockTimeProcessor inBlockTimeProcessor;

    @Autowired
    private LandingTimeProcessor landingTimeProcessor;

    @Autowired
    private OffBlockTimeProcessor offBlockTimeProcessor;

    @Autowired
    private TakeOffTimeProcessor takeOffTimeProcessor;

    @Autowired
    public void kAggregateStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        // stream serde

        JsonSerde<DelayEvent> delayJsonSerde = new JsonSerde<>(DelayEvent.class, objectMapper);
        JsonSerde<DiversionEvent> diversionSerde = new JsonSerde<>(DiversionEvent.class, objectMapper);
        JsonSerde<DoorCloseEvent> doorJsonSerde = new JsonSerde<>(DoorCloseEvent.class, objectMapper);
        JsonSerde<EquipmentEvent> equipmentJsonSerde = new JsonSerde<>(EquipmentEvent.class, objectMapper);
        JsonSerde<FlightCancelEvent> flightCancelJsonSerde = new JsonSerde<>(FlightCancelEvent.class, objectMapper);
        JsonSerde<FlightDeleteEvent> flightDeleteJsonSerde = new JsonSerde<>(FlightDeleteEvent.class, objectMapper);
        JsonSerde<FlightReturnEvent> flightReturnJsonSerde = new JsonSerde<>(FlightReturnEvent.class, objectMapper);
        JsonSerde<GateChangeEvent> gateChangeJsonSerde = new JsonSerde<>(GateChangeEvent.class, objectMapper);
        JsonSerde<InBlockTimeEvent> inBlockTimeJsonSerde = new JsonSerde<>(InBlockTimeEvent.class, objectMapper);
        JsonSerde<LandingTimeEvent> landingTimeJsonSerde = new JsonSerde<>(LandingTimeEvent.class, objectMapper);
        JsonSerde<OffBlockTimeEvent> offBlockTimeJsonSerde = new JsonSerde<>(OffBlockTimeEvent.class, objectMapper);
        JsonSerde<TakeOffTimeEvent> takeOffTimeJsonSerde = new JsonSerde<>(TakeOffTimeEvent.class, objectMapper);

        // this Multiple String Serde -- coming record from here

        JsonSerde<AggregationEvent> aggregateJsonSerde = new JsonSerde<>(AggregationEvent.class, objectMapper);

        // this is my Agg serde

        JsonSerde<CountAggregation> combinedJsonSerde = new JsonSerde<>(CountAggregation.class, objectMapper);


        KStream<String, DelayEvent> delayEvent =
                builder.stream("delayEvent", Consumed.with(stringSerde, delayJsonSerde));

        KStream<String, DiversionEvent> diversionEvent =
                builder.stream("diversionEvent", Consumed.with(stringSerde, diversionSerde));

        KStream<String, DoorCloseEvent> doorCloseEvent =
                builder.stream("doorClose", Consumed.with(stringSerde, doorJsonSerde));

        KStream<String, EquipmentEvent> equipmentEvent =
                builder.stream("equipment", Consumed.with(stringSerde, equipmentJsonSerde));

        KStream<String, FlightCancelEvent> flightCancelEvent =
                builder.stream("flightCancel", Consumed.with(stringSerde, flightCancelJsonSerde));

        KStream<String, FlightDeleteEvent> flightDeleteEvent =
                builder.stream("flightDelete", Consumed.with(stringSerde, flightDeleteJsonSerde));

        KStream<String, FlightReturnEvent> flightReturnEvent =
                builder.stream("flightReturn", Consumed.with(stringSerde, flightReturnJsonSerde));

        KStream<String, GateChangeEvent> gateChangeEvent =
                builder.stream("gateChange", Consumed.with(stringSerde, gateChangeJsonSerde));

        KStream<String, InBlockTimeEvent> inBlockTimeEventK =
                builder.stream("inBlockTime", Consumed.with(stringSerde, inBlockTimeJsonSerde));

        KStream<String, LandingTimeEvent> landingTimeEvent =
                builder.stream("landingTime", Consumed.with(stringSerde, landingTimeJsonSerde));

        KStream<String, OffBlockTimeEvent> offBlockTimeEvent =
                builder.stream("offBlockTime", Consumed.with(stringSerde, offBlockTimeJsonSerde));

        KStream<String, TakeOffTimeEvent> takeOffTimeEvent =
                builder.stream("takeOffTime", Consumed.with(stringSerde, takeOffTimeJsonSerde));

        KStream<String, AggregationEvent> delayAggregate =
                delayEvent.mapValues(delay -> new AggregationEvent(delay.getFlightInfo(), delay, null, null, null, null, null, null, null, null, null, null, null))
                        .selectKey(
                                (k, v) -> v.getDelayEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null); // remove only null key

        KStream<String, AggregationEvent> diversionAggregate =
                diversionEvent.mapValues(diversion ->
                                new AggregationEvent(diversion.getFlightInfo(), null, diversion, null, null, null, null, null, null, null, null, null, null))
                        .selectKey((k, v) -> v.getDiversionEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);

        KStream<String, AggregationEvent> doorCloseAggregation =
                doorCloseEvent.mapValues(doorClose -> new AggregationEvent(doorClose.getFlightInfo(), null, null, doorClose, null, null, null, null, null, null, null, null, null))
                        .selectKey((k, v) -> v.getDoorCloseEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);

        KStream<String, AggregationEvent> equipmentAggregation =
                equipmentEvent.mapValues(equipment -> new AggregationEvent(equipment.getFlightInfo(), null, null, null, equipment, null, null, null, null, null, null, null, null))
                        .selectKey((k, v) -> v.getEquipmentEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


//        KStream<String, AggregationEvent> doorAggregate =
//                doorCloseEvent.mapValues(doorClose -> new AggregationEvent(doorClose.getFlightInfo(), null, null, doorClose, null, null, null, null, null, null, null, null, null))
//                        .selectKey((k, v) -> v.getDoorCloseEvent() != null ? v.getDoorCloseEvent().getFlightInfo().getLegId() : null)
//                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> flightCancelAggregation =
                flightCancelEvent.mapValues(flightCancel -> new AggregationEvent(flightCancel.getFlightInfo(), null, null, null, null, flightCancel, null, null, null, null, null, null, null))
                        .selectKey((k, v) -> v.getFlightCancelEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> flightDeleteAggregate =
                flightDeleteEvent.mapValues(flightDelete ->
                                new AggregationEvent(flightDelete.getFlightInfo(), null, null, null, null, null, flightDelete, null, null, null, null, null, null))
                        .selectKey((k, v) -> v.getFlightDelete() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> flightReturnAggregate =
                flightReturnEvent.mapValues(
                                flightReturn -> new AggregationEvent(flightReturn.getFlightInfo(), null, null, null, null, null, null, flightReturn, null, null, null, null, null))
                        .selectKey((k, v) -> v.getFlightReturnEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> gateChangeAggregate =
                gateChangeEvent.mapValues(gateChange -> new AggregationEvent(gateChange.getFlightInfo(), null, null, null, null, null, null, null, gateChange, null, null, null, null))
                        .selectKey((k, v) -> v.getGateChangeEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> inBlockTimeAggregate =
                inBlockTimeEventK.mapValues(inBlockTime -> new AggregationEvent(inBlockTime.getFlightInfo(), null, null, null, null, null, null, null, null, inBlockTime, null, null, null))
                        .selectKey((k, v) -> v.getInBlockTimeEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> landingTimeAggregate =
                landingTimeEvent.mapValues(landingTime -> new AggregationEvent(landingTime.getFlightInfo(), null, null, null, null, null, null, null, null, null, landingTime, null, null))
                        .selectKey((k, v) -> v.getLandingTimeEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);

        KStream<String, AggregationEvent> offBlockTimeAggregate =
                offBlockTimeEvent.mapValues(offBlockTime -> new AggregationEvent(offBlockTime.getFlightInfo(), null, null, null, null, null, null, null, null, null, null, offBlockTime, null))
                        .selectKey((k, v) -> v.getOffBlockTimeEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);


        KStream<String, AggregationEvent> takeOffTimeAggregate =
                takeOffTimeEvent.mapValues(takeOffTime -> new AggregationEvent(takeOffTime.getFlightInfo(), null, null, null, null, null, null, null, null, null, null, null, takeOffTime))
                        .selectKey((k, v) -> v.getTakeOffTimeEvent() != null ? v.getFlightInfo().getLegId() : null)
                        .filter((k, v) -> k != null);

        // Step 3: Merge All Stream
        KStream<String, AggregationEvent> merged = delayAggregate
                .merge(diversionAggregate)
                .merge(doorCloseAggregation)
                .merge(equipmentAggregation)
                .merge(flightCancelAggregation)
                .merge(flightDeleteAggregate)
                .merge(flightReturnAggregate)
                .merge(gateChangeAggregate)
                .merge(inBlockTimeAggregate)
                .merge(landingTimeAggregate)
                .merge(offBlockTimeAggregate)
                .merge(takeOffTimeAggregate);


        // Step 4: Aggregate
        KTable<String, CountAggregation> aggregatedTable =
                merged.groupByKey(Grouped.with(Serdes.String(), aggregateJsonSerde))
                        .aggregate(() -> new CountAggregation(),
                                (key, value, agg) -> {


                                    // If master is empty, take the first record as master
                                    if (agg.getFlightInfo() == null) {
                                        agg.setFlightInfo(value.getFlightInfo());
                                    }

                                    // Only update if incoming legId matches master legId
                                    if (!agg.getFlightInfo().getLegId().equals(value.getFlightInfo().getLegId())) {
                                        return agg; // ignore record
                                    }

                                    if (value.getDelayEvent() != null) {
                                        agg.setDelayEvent(delayProcessor.delayEventProcessing(value.getDelayEvent(), agg.getDelayEvent()));
                                    }

                                    if (value.getDiversionEvent() != null) {
                                        agg.setDiversionEvent(diversionProcessor.diversionEventProcessor(value.getDiversionEvent(), agg.getDiversionEvent()));
                                    }


                                    if (value.getDoorCloseEvent() != null) {
                                        agg.setDoorClose(doorProcessor.doorCloseProcessor(value.getDoorCloseEvent(), agg.getDoorClose()));
                                    }

                                    if (value.getFlightCancelEvent() != null) {
                                        agg.setFlightCancelEvent(flightCancelProcessor.flightCancelEvent(value.getFlightCancelEvent(), agg.getFlightCancelEvent()));
                                    }

                                    if (value.getFlightDelete() != null) {
                                        agg.setFlightDelete(flightDeleteProcessor.flightDeleteProcessorEvent(value.getFlightDelete(), agg.getFlightDelete()));
                                    }

                                    if (value.getFlightReturnEvent() != null) {
                                        agg.setFlightReturnEvent(flightReturnProcessor.flightReturnEvent(value.getFlightReturnEvent(), agg.getFlightReturnEvent()));
                                    }

                                    if (value.getGateChangeEvent() != null) {
                                        agg.setGateChangeEvent(gateChangeEventProcessor.gateChangeEventProcessor(value.getGateChangeEvent(), agg.getGateChangeEvent()));
                                    }

                                    if (value.getInBlockTimeEvent() != null) {
                                        agg.setInBlockTimeEvent(inBlockTimeProcessor.inBlockTimeEvent(value.getInBlockTimeEvent(), agg.getInBlockTimeEvent()));
                                    }

                                    if (value.getLandingTimeEvent() != null) {
                                        agg.setLandingTimeEvent(landingTimeProcessor.landingTimeEvent(value.getLandingTimeEvent(), agg.getLandingTimeEvent()));
                                    }

                                    if (value.getOffBlockTimeEvent() != null) {
                                        agg.setOffBlockTimeEvent(offBlockTimeProcessor.offBlockTimeEvent(value.getOffBlockTimeEvent(), agg.getOffBlockTimeEvent()));
                                    }

                                    if (value.getTakeOffTimeEvent() != null) {
                                        agg.setTakeOffTimeEvent(takeOffTimeProcessor.takeOffTimeEvent(value.getTakeOffTimeEvent(), agg.getTakeOffTimeEvent()));
                                    }


                                    return agg;
                                }, Materialized.<String, CountAggregation, KeyValueStore<Bytes, byte[]>>as("combined-store")
                                        .withKeySerde(stringSerde)
                                        .withValueSerde(combinedJsonSerde));

        aggregatedTable.toStream().to("combinedAgg", Produced.with(stringSerde, combinedJsonSerde));

        aggregatedTable.toStream().foreach(
                (key, value) -> {
                    try {
                        String json = objectMapper.writeValueAsString(value);
                        redisTemplate.opsForValue().set(key, json);
                        System.out.println("Redis for key: " + key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
        );
    }
    // objectMapper.writeValueAsString -> this method used to use for convert java object into JSON String
    // redisTemplate.opsForValue() -> this method used to store key-value format . it provide method like get, set
}
