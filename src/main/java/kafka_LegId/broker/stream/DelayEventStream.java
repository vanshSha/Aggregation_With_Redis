package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.DelayEvent;
import kafka_LegId.broker.stream.processor.DelayEventProcessor;
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
public class DelayEventStream {

    private final DelayEventProcessor delayEventProcessor;

    public DelayEventStream(DelayEventProcessor delayEventProcessor) {
        this.delayEventProcessor = delayEventProcessor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void KStreamDelayEvent(StreamsBuilder streamBuilder) {

        Serde<String> stringSerde = Serdes.String();
        JsonSerde<DelayEvent> delayEventJsonSerde = new JsonSerde<>(DelayEvent.class, objectMapper);

        KStream<String, DelayEvent> delayEvent =
                streamBuilder.stream("delayEvent", Consumed.with(stringSerde, delayEventJsonSerde));

        KStream<String, DelayEvent> keyDelayEventStream =
                delayEvent.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey(
                                (k, v) -> v.getFlightInfo().getLegId());

        KTable<String, DelayEvent> stateFullTable =
                streamBuilder.table("processedDelayEvent", Consumed.with(stringSerde, delayEventJsonSerde),
                        Materialized.<String, DelayEvent, KeyValueStore<Bytes, byte[]>>as("delayEvent")
                                .withKeySerde(stringSerde)
                                .withValueSerde(delayEventJsonSerde));

        KStream<String, DelayEvent> processedStream = keyDelayEventStream.leftJoin(
                stateFullTable,
                (incomingEvent, existingEvent) -> {
                    if (existingEvent == null
                            || !existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())
                    ) {
                        System.out.println("CREATE: legId = " + incomingEvent.getFlightInfo().getLegId());
                    } else {
                        System.out.println("UPDATE: legId = " + incomingEvent.getFlightInfo().getLegId());
                    }
                    DelayEvent result = delayEventProcessor.delayEventProcessing(incomingEvent, existingEvent);

                    return result;
                }
        );
        processedStream.to("processedDelayEvent", Produced.with(stringSerde, delayEventJsonSerde));


    }
}