package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.FlightCancelEvent;
import kafka_LegId.broker.stream.processor.FlightCancelProcessor;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

//@Component
public class FlightCancelStream {

    private final FlightCancelProcessor flightCancelProcessor;

    public FlightCancelStream(FlightCancelProcessor flightCancelProcessor) {
        this.flightCancelProcessor = flightCancelProcessor;
    }

    @Autowired
    public void KFlightCancelStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<FlightCancelEvent> flightCancelEventJsonSerde = new JsonSerde<>(FlightCancelEvent.class);

        // consume
        KStream<String, FlightCancelEvent> flightCancelEvent =
                builder.stream("flightCancel", Consumed.with(stringSerde, flightCancelEventJsonSerde));


        KStream<String, FlightCancelEvent> flightCancelStream =
                flightCancelEvent.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        KTable<String, FlightCancelEvent> flightCancelTable =
                builder.table("processedFlightCancel", Consumed.with(stringSerde, flightCancelEventJsonSerde),
                        Materialized.<String, FlightCancelEvent, KeyValueStore<Bytes, byte[]>>as("flightCancelStore")
                                .withKeySerde(stringSerde)
                                .withValueSerde(flightCancelEventJsonSerde));

        KStream<String, FlightCancelEvent> flightCancelProStream =
                flightCancelStream.leftJoin(flightCancelTable,
                        (inComingEvent, existingEvent) -> {

                            String inComingLegId = inComingEvent != null && inComingEvent.getFlightInfo() != null ?
                                    inComingEvent.getFlightInfo().getLegId() : null;

                            String existingLegId = existingEvent != null && existingEvent.getFlightInfo() != null ?
                                    existingEvent.getFlightInfo().getLegId() : null;

                            if (existingLegId == null || !existingLegId.equals(inComingLegId)) {
                                System.out.println("CREATE: legId = " + inComingLegId);
                            } else {
                                System.out.println("UPDATE: legId = " + inComingLegId);

                            }
                            FlightCancelEvent flCancel = flightCancelProcessor.flightCancelEvent(inComingEvent, existingEvent);
                            return flCancel;
                        });

        flightCancelProStream.to("processedFlightCancel", Produced.with(stringSerde, flightCancelEventJsonSerde));
    }
}
