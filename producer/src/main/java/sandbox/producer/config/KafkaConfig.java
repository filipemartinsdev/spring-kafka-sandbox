package sandbox.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration @Profile("!test")
public class KafkaConfig {

    @Bean
    public NewTopic temperatureTopic() {
        return TopicBuilder.name("sensor.temperature.updated")
                .replicas(1)
                .partitions(1)
                .build();
    }
}
