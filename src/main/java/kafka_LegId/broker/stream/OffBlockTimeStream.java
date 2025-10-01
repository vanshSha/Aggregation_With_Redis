package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.OffBlockTimeEvent;
import kafka_LegId.broker.stream.processor.OffBlockTimeProcessor;
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
public class OffBlockTimeStream {

    private final OffBlockTimeProcessor processor;

    public OffBlockTimeStream(OffBlockTimeProcessor processor) {
        this.processor = processor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void offBlockTimeStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<OffBlockTimeEvent> offBlockTimeEventJsonSerde = new JsonSerde<>(OffBlockTimeEvent.class, objectMapper);

        KStream<String, OffBlockTimeEvent> rawStream =
                builder.stream("blockTime", Consumed.with(stringSerde, offBlockTimeEventJsonSerde));

        KStream<String, OffBlockTimeEvent> filterStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        KTable<String, OffBlockTimeEvent> offBlockTimeEventKTable =
                builder.table("processedBlockTime", Consumed.with(stringSerde, offBlockTimeEventJsonSerde),
                        Materialized.<String, OffBlockTimeEvent, KeyValueStore<Bytes, byte[]>>as("FilterBlock")
                                .withKeySerde(stringSerde)
                                .withValueSerde(offBlockTimeEventJsonSerde));

        KStream<String, OffBlockTimeEvent> processedOffBlockTime =
                filterStream.leftJoin(
                        offBlockTimeEventKTable,
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

                            OffBlockTimeEvent offBlockTimeEvent = processor.offBlockTimeEvent(incomingEvent, existingEvent);
                            return offBlockTimeEvent;
                        });


        processedOffBlockTime.to("processedBlockTime", Produced.with(stringSerde, offBlockTimeEventJsonSerde));
    }
}
