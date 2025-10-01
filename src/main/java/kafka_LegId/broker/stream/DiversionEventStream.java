package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.DiversionEvent;
import kafka_LegId.broker.stream.processor.DiversionEventProcessor;
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
public class DiversionEventStream {

    private final DiversionEventProcessor diversionEventProcessor;

    public DiversionEventStream(DiversionEventProcessor diversionEventProcessor) {
        this.diversionEventProcessor = diversionEventProcessor;
    }

    @Autowired
    public void KStreamDiversionEvent(StreamsBuilder streamsBuilder) {

        Serde<String> stringSerde = Serdes.String(); // key

        JsonSerde<DiversionEvent> diversionEventJsonSerde = new JsonSerde<>(DiversionEvent.class);

        // Consumer part
        KStream<String, DiversionEvent> DiversionEvent =
                streamsBuilder.stream("diversion", Consumed.with(stringSerde, diversionEventJsonSerde));

        KStream<String, DiversionEvent> diversionStream =
                DiversionEvent.filter(
                                (k, v) ->
                                        v.getFlightInfo() != null &&
                                                v.getFlightInfo().getLegId() != null &&
                                                !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey(
                                (k, v) -> v.getFlightInfo().getLegId());


        KTable<String, DiversionEvent> diversionTable =
                streamsBuilder.table("diversionEvent", Consumed.with(stringSerde, diversionEventJsonSerde),
                        Materialized.<String, DiversionEvent, KeyValueStore<Bytes, byte[]>>as("diversionStore")
                                .withKeySerde(stringSerde)
                                .withValueSerde(diversionEventJsonSerde));


        KStream<String, DiversionEvent> processedStream = diversionStream.leftJoin(
                diversionTable,
                (incomingEvent, existingEvent) -> {
                    if (existingEvent == null
                            || !existingEvent.getFlightInfo().getLegId().equals(incomingEvent.getFlightInfo().getLegId())
                    ) {
                        System.out.println("CREATE: legId = " + incomingEvent.getFlightInfo().getLegId());
                    } else {
                        System.out.println("UPDATE: legId = " + incomingEvent.getFlightInfo().getLegId());
                    }
                    DiversionEvent delayEvent = diversionEventProcessor.diversionEventProcessor(incomingEvent, existingEvent);
                    return delayEvent;
                }
        );
        processedStream.to("diversionEvent", Produced.with(stringSerde, diversionEventJsonSerde));

    }
}
