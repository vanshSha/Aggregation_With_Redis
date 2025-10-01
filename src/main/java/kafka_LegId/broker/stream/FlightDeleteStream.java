package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.FlightDeleteEvent;
import kafka_LegId.broker.stream.processor.FlightDeleteProcessor;
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
public class FlightDeleteStream {

    private final FlightDeleteProcessor deleteEventProcessor;

    public FlightDeleteStream(FlightDeleteProcessor deleteEventProcessor) {
        this.deleteEventProcessor = deleteEventProcessor;
    }


    @Autowired
    public void kFlightDeleteStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<FlightDeleteEvent> flightDeleteEventJsonSerde = new JsonSerde<>(FlightDeleteEvent.class);

        // read raw data
        KStream<String, FlightDeleteEvent> flightDeleteEvent =
                builder.stream("flightDelete", Consumed.with(stringSerde, flightDeleteEventJsonSerde));

        // perform some action
        KStream<String, FlightDeleteEvent> flightDeleteStream =
                flightDeleteEvent.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        KTable<String, FlightDeleteEvent> flightDeleteTable =
                builder.table("processedFlightDelete", Consumed.with(stringSerde, flightDeleteEventJsonSerde),
                        Materialized.<String, FlightDeleteEvent, KeyValueStore<Bytes, byte[]>>as("flightDeleteStore")
                                .withKeySerde(stringSerde)
                                .withValueSerde(flightDeleteEventJsonSerde));

        KStream<String, FlightDeleteEvent> flightDeleteProStream =
                flightDeleteStream.leftJoin(
                        flightDeleteTable,
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
                            FlightDeleteEvent fDelete = deleteEventProcessor.flightDeleteProcessorEvent(inComingEvent, existingEvent);
                            return fDelete;
                        });
        flightDeleteProStream.to("processedFlightDelete", Produced.with(stringSerde, flightDeleteEventJsonSerde));
    }

}
