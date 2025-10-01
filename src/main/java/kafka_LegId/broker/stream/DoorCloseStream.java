package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.DoorCloseEvent;
import kafka_LegId.broker.stream.processor.DoorCloseProcessor;
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
public class DoorCloseStream {

    private final DoorCloseProcessor doorCloseProcessor;

    public DoorCloseStream(DoorCloseProcessor doorCloseProcessor) {
        this.doorCloseProcessor = doorCloseProcessor;
    }


    @Autowired
    public void KStreamDoorClose(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<DoorCloseEvent> doorCloseStreamJsonSerde = new JsonSerde<>(DoorCloseEvent.class);

        KStream<String, DoorCloseEvent> doorCloseEvent =
                streamsBuilder.stream("doorClose", Consumed.with(stringSerde, doorCloseStreamJsonSerde));


        KStream<String, DoorCloseEvent> doorCloseStream =
                doorCloseEvent
                        .filter((k, v) -> v.getFlightInfo() != null
                                && v.getFlightInfo().getLegId() != null
                                && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());

        // table
        KTable<String, DoorCloseEvent> doorCloseTable =
                streamsBuilder.table("processedCloseDoor", Consumed.with(stringSerde, doorCloseStreamJsonSerde),
                        Materialized.<String, DoorCloseEvent, KeyValueStore<Bytes, byte[]>>as("door-close-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(doorCloseStreamJsonSerde)); // how to read record from topic


        // join
        KStream<String, DoorCloseEvent> processedStream =
                doorCloseStream.leftJoin(doorCloseTable, (incomingEvent, existingEvent) -> {

                    String incomingLegID = incomingEvent != null && incomingEvent.getFlightInfo() != null ?
                            incomingEvent.getFlightInfo().getLegId() : null;

                    String existingLegID = existingEvent != null && existingEvent.getFlightInfo() != null ?
                            existingEvent.getFlightInfo().getLegId() : null;

                    if (existingLegID == null || !existingLegID.equals(incomingLegID)) {
                        System.out.println("CREATE: legId = " + incomingLegID);
                    } else {
                        System.out.println("UPDATE: legId = " + incomingLegID);
                    }
                    DoorCloseEvent dorCloseEvent = doorCloseProcessor.doorCloseProcessor(incomingEvent, existingEvent);
                    return dorCloseEvent;
                });

        processedStream.to("processedCloseDoor", Produced.with(stringSerde, doorCloseStreamJsonSerde));


    }
}
