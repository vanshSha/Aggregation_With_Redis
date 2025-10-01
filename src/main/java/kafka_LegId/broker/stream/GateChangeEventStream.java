package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.GateChangeEvent;
import kafka_LegId.broker.stream.processor.GateChangeEventProcessor;
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
public class GateChangeEventStream {

    private final GateChangeEventProcessor processor;

    public GateChangeEventStream(GateChangeEventProcessor processor) {
        this.processor = processor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void kGateChangEvent(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<GateChangeEvent> gateChangeEventJson = new JsonSerde<>(GateChangeEvent.class, objectMapper);

        // Raw Stream
        KStream<String, GateChangeEvent> rawStream =
                builder.stream("gateChange", Consumed.with(stringSerde, gateChangeEventJson));


        // Filter Stream
        KStream<String, GateChangeEvent> filterStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey(
                                (k, v) -> v.getFlightInfo().getLegId());



        // KTable
        KTable<String, GateChangeEvent> gateChangeEventKTable =
                builder.table("processedGateChange", Consumed.with(stringSerde, gateChangeEventJson),
                        Materialized.<String, GateChangeEvent, KeyValueStore<Bytes, byte[]>>as("flightGateChange")
                                .withKeySerde(stringSerde)
                                .withValueSerde(gateChangeEventJson));

        // Processed KStreams
        KStream<String, GateChangeEvent> processedKStream =
                filterStream.leftJoin(
                        gateChangeEventKTable,
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

                            GateChangeEvent changeEvent = processor.gateChangeEventProcessor(incomingEvent, existingEvent);
                            return changeEvent;
                        });

        processedKStream.to("processedGateChange", Produced.with(stringSerde, gateChangeEventJson));
    }
}
