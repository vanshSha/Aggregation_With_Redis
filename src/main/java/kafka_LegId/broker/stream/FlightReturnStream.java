package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.FlightReturnEvent;
import kafka_LegId.broker.stream.processor.FlightReturnProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
// THIS CLASS KSQLDB ISSUE
//@Component
public class FlightReturnStream {

    private final FlightReturnProcessor flightReturnProcessor;

    public FlightReturnStream(FlightReturnProcessor flightReturnProcessor) {
        this.flightReturnProcessor = flightReturnProcessor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void kFlightReturnStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<FlightReturnEvent> flightReturnEventJsonSerde = new JsonSerde<>(FlightReturnEvent.class, objectMapper);

        KStream<String, FlightReturnEvent> rawStream =
                builder.stream("flightReturn", Consumed.with(stringSerde, flightReturnEventJsonSerde));


        KStream<String, FlightReturnEvent> flightReturnStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());



        KTable<String, FlightReturnEvent> flightReturnTable =
                builder.table("processedFlightReturn", Consumed.with(stringSerde, flightReturnEventJsonSerde),
                        Materialized.<String, FlightReturnEvent, KeyValueStore<Bytes, byte[]>>as("flightReturn")
                                .withKeySerde(stringSerde)
                                .withValueSerde(flightReturnEventJsonSerde));


        KStream<String, FlightReturnEvent> processedStream =
                flightReturnStream.leftJoin(flightReturnTable,
                        (incomingEvent, existingEvent) -> {

                            String inComingLegId = incomingEvent != null && incomingEvent.getFlightInfo() != null ?
                                    incomingEvent.getFlightInfo().getLegId() : null;
                            String existingLegId = existingEvent != null && existingEvent.getFlightInfo() != null ?
                                    existingEvent.getFlightInfo().getLegId() : null;

                            if (existingLegId == null || !existingLegId.equals(inComingLegId)) {
                                System.out.println("CREATE: legId = " + inComingLegId);
                            } else {
                                System.out.println("UPDATE: legId = " + inComingLegId);
                            }
                            FlightReturnEvent returnEvent = flightReturnProcessor.flightReturnEvent(incomingEvent, existingEvent);
                            return returnEvent;
                        });

        processedStream.to("processedFlightReturn", Produced.with(stringSerde, flightReturnEventJsonSerde));

    }
}
