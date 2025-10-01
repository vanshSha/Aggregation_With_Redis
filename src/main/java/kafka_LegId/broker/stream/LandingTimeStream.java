package kafka_LegId.broker.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka_LegId.broker.event.LandingTimeEvent;
import kafka_LegId.broker.stream.processor.LandingTimeProcessor;
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
public class LandingTimeStream {

    private final LandingTimeProcessor processor;


    public LandingTimeStream(LandingTimeProcessor processor) {
        this.processor = processor;
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private void landingTimeStream(StreamsBuilder builder) {


        Serde<String> stringSerde = Serdes.String();

        JsonSerde<LandingTimeEvent> landingTimeEventJsonSerde = new JsonSerde<>(LandingTimeEvent.class, objectMapper);

        KStream<String, LandingTimeEvent> rawStream =
                builder.stream("landingRawTimeEvent", Consumed.with(stringSerde, landingTimeEventJsonSerde));

        KStream<String, LandingTimeEvent> filterStream =
                rawStream.filter(
                                (k, v) -> v.getFlightInfo() != null
                                        && v.getFlightInfo().getLegId() != null
                                        && !v.getFlightInfo().getLegId().isEmpty())
                        .selectKey((k, v) -> v.getFlightInfo().getLegId());


        KTable<String, LandingTimeEvent> landingTimeEventKTable =
                builder.table("filterLandingTimeEvent", Consumed.with(stringSerde, landingTimeEventJsonSerde),
                        Materialized.<String, LandingTimeEvent, KeyValueStore<Bytes, byte[]>>as("landTime")
                                .withKeySerde(stringSerde)
                                .withValueSerde(landingTimeEventJsonSerde));

        KStream<String, LandingTimeEvent> processedFilterLanding =
                filterStream.leftJoin(landingTimeEventKTable,
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
                            LandingTimeEvent landingTimeEvent = processor.landingTimeEvent(incomingEvent, existingEvent);
                            return landingTimeEvent;
                        });

        processedFilterLanding.to("processedLandingTimeEvent", Produced.with(stringSerde, landingTimeEventJsonSerde));
    }
}
