package sandbox.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import sandbox.producer.application.dto.event.NewTemperatureEvent;
import sandbox.producer.application.dto.event.TemperatureAlertEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Configuration @Profile("!test")
@EnableKafkaStreams
public class KafkaConfig {
    @Value("${app.alerts.temperature-limit:40}")
    private float TEMP_LIMIT;

    @Bean
    public NewTopic temperatureTopic() {
        return TopicBuilder.name("sensor.temperature.updated")
                .replicas(1)
                .partitions(1)
                .build();
    }

    @Bean
    public NewTopic temperatureAlertsTopic() {
        return TopicBuilder.name("sensor.temperature.alerts")
                .replicas(1)
                .partitions(1)
                .build();
    }

//    Serde = Serializer & Deserializer

    @Bean
    public KStream<String, NewTemperatureEvent> temperatureTelemetryStream(StreamsBuilder streamsBuilder) {
//        Step 1: create Serde
        JacksonJsonSerde<NewTemperatureEvent> eventSerde = new JacksonJsonSerde<>(NewTemperatureEvent.class);
        JacksonJsonSerde<TemperatureAccumulator> accumSerde = new JacksonJsonSerde<>(TemperatureAccumulator.class);
        JacksonJsonSerde<TemperatureAlertEvent> alertSerde = new JacksonJsonSerde<>(TemperatureAlertEvent.class);

//        Step 2: Stream entry
        KStream<String, NewTemperatureEvent> sourceStream = streamsBuilder.stream(
                "sensor.temperature.updated",
                Consumed.with(Serdes.String(), eventSerde)
        );

//        Step 3: Sliding Window of 5 minutes
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));

//        Step 4: Pipeline definition
        sourceStream
//                Group entries by ID
                .groupBy(
                        (key, event) -> event.componentId().toString(),
                        Grouped.with(Serdes.String(), eventSerde)
                )

                .windowedBy(timeWindows)

//                Stateless -> Stateful
                .aggregate(
                        () -> new TemperatureAccumulator(0, 0L), // Initial value
                        (key, event, accum) -> {
                            return new TemperatureAccumulator(accum.sum() + event.value(), accum.count() + 1);
                        },
                        Materialized.with(Serdes.String(), accumSerde)
                )

                .toStream()

//                Calculate exceeded temperatures
                .filter((windowedKey, accum) -> accum.getAverage() > TEMP_LIMIT)

                .map((windowedKey, accum) -> {
                    String componentId = windowedKey.key();
                    String alertMessage = String.format(
                            "[ALERT] The sensor %s exceeded the temperature limit. Last 5 minutes average: %.2fº",
                            componentId, accum.getAverage()
                    );
                    return new KeyValue<>(componentId, new TemperatureAlertEvent(UUID.fromString(componentId), alertMessage, Instant.now()));
                })

                .to("sensor.temperature.alerts", Produced.with(Serdes.String(), alertSerde));

        return sourceStream;
    }


    private static record TemperatureAccumulator (float sum, long count){
        public float getAverage(){
            return count == 0 ? 0.0f : sum / count;
        }
    };
}

