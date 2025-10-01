package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.TakeOffTimeEvent;
import kafka_LegId.broker.stream.processor.TakeOffTimeProcessor;
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
public class TakeOffTimeStream {

    private final TakeOffTimeProcessor processor;

    public TakeOffTimeStream(TakeOffTimeProcessor processor) {
        this.processor = processor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    public void takeOffTimeStream(StreamsBuilder builder) {

        Serde<String> stringSerde = Serdes.String();

        JsonSerde<TakeOffTimeEvent> takeOffTimeEventJsonSerde = new JsonSerde<>(TakeOffTimeEvent.class, objectMapper);

        KStream<String, TakeOffTimeEvent> rawStream =
                builder.stream("takeOffTime", Consumed.with(stringSerde, takeOffTimeEventJsonSerde));

        KStream<String, TakeOffTimeEvent> filterStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        KTable<String, TakeOffTimeEvent> takeOffTimeEventKTable =
                builder.table("processedTakeTime",
                        Consumed.with(stringSerde, takeOffTimeEventJsonSerde),
                        Materialized.<String, TakeOffTimeEvent, KeyValueStore<Bytes, byte[]>>as("TakeOffStore")
                                .withKeySerde(stringSerde)
                                .withValueSerde(takeOffTimeEventJsonSerde));


        KStream<String, TakeOffTimeEvent> processedTakeOffTime =
                filterStream.leftJoin(
                        takeOffTimeEventKTable,
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

                            TakeOffTimeEvent takeOffTimeEvent = processor.takeOffTimeEvent(incomingEvent, existingEvent);
                            return takeOffTimeEvent;
                        });

        processedTakeOffTime.to("processedTakeTime", Produced.with(stringSerde, takeOffTimeEventJsonSerde));

    }

}

