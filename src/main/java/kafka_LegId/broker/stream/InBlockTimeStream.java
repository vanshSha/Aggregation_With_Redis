package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.InBlockTimeEvent;
import kafka_LegId.broker.stream.processor.InBlockTimeProcessor;
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
public class InBlockTimeStream {

    private final InBlockTimeProcessor processor;

    public InBlockTimeStream(InBlockTimeProcessor processor) {
        this.processor = processor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void inBlockTimeStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<InBlockTimeEvent> inBlockTimeStreamJsonSerde = new JsonSerde<>(InBlockTimeEvent.class, objectMapper);


        KStream<String, InBlockTimeEvent> rawStream =
                builder.stream("inBlockEvent", Consumed.with(stringSerde, inBlockTimeStreamJsonSerde));


        KStream<String, InBlockTimeEvent> filterStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey(
                                (k, v) -> v.getFlightInfo().getLegId());

        KTable<String, InBlockTimeEvent> inBlockTimeEventKTable =
                builder.table("processedBlockTime", Consumed.with(stringSerde, inBlockTimeStreamJsonSerde),
                        Materialized.<String, InBlockTimeEvent, KeyValueStore<Bytes, byte[]>>as("InBlock")
                                .withKeySerde(stringSerde)
                                .withValueSerde(inBlockTimeStreamJsonSerde));

        KStream<String, InBlockTimeEvent> processedInBlockEvent =
                filterStream.leftJoin(
                        inBlockTimeEventKTable,
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


                            InBlockTimeEvent blockTimeEvent = processor.inBlockTimeEvent(incomingEvent, existingEvent);
                            return blockTimeEvent;
                        });

        processedInBlockEvent.to("processedBlockTime", Produced.with(stringSerde, inBlockTimeStreamJsonSerde));
    }
}
