package kafka_LegId.broker.stream;

import kafka_LegId.broker.event.EquipmentEvent;
import kafka_LegId.broker.stream.processor.EquipmentProcessor;
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
public class EquipmentStream {

    private final EquipmentProcessor equipmentProcessors;

    public EquipmentStream(EquipmentProcessor equipmentProcessor) {
        this.equipmentProcessors = equipmentProcessor;
    }

    @Autowired
    public void kStreamEquipment(StreamsBuilder streamsBuilder) {

        Serde<String> stringSerde = Serdes.String();
        JsonSerde<EquipmentEvent> jsonEquipmentSerde = new JsonSerde<>(EquipmentEvent.class);

        KStream<String, EquipmentEvent> equipmentEvent =
                streamsBuilder.stream("equipment", Consumed.with(stringSerde, jsonEquipmentSerde));


        KStream<String, EquipmentEvent> equipmentStream =
                equipmentEvent.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        // kTable is reading record from  processedStream
        KTable<String, EquipmentEvent> equipmentTable =
                streamsBuilder.table("processedEquipment", Consumed.with(stringSerde, jsonEquipmentSerde),
                        Materialized.<String, EquipmentEvent, KeyValueStore<Bytes, byte[]>>as("equipment")
                                .withKeySerde(stringSerde)
                                .withValueSerde(jsonEquipmentSerde));


        KStream<String, EquipmentEvent> processedEquipmentStream =
                equipmentStream.leftJoin(equipmentTable,
                        (inComingEvent, existingEvent) -> {

                            String incomingLegId = inComingEvent != null && inComingEvent.getFlightInfo() != null ?
                                    inComingEvent.getFlightInfo().getLegId() : null;

                            String existingLegId = existingEvent != null && existingEvent.getFlightInfo() != null ?
                                    existingEvent.getFlightInfo().getLegId() : null;

                            if (existingLegId == null || !existingLegId.equals(incomingLegId)) {
                                System.out.println("CREATE: legId = " + incomingLegId);
                            } else {
                                System.out.println("UPDATE: legId = " + incomingLegId);
                            }

                            EquipmentEvent eqEvent = equipmentProcessors.equipmentProcessor(inComingEvent, existingEvent);
                            return eqEvent;
                        });

        processedEquipmentStream.to("processedEquipment", Produced.with(stringSerde, jsonEquipmentSerde));

    }

}
