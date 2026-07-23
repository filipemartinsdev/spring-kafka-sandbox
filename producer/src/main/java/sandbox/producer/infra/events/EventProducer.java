package sandbox.producer.infra.events;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import sandbox.producer.application.dto.event.NewTemperatureEvent;

@Slf4j
@Component
public class EventProducer {

    private final KafkaTemplate<String, NewTemperatureEvent> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, NewTemperatureEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceNewTemperature(NewTemperatureEvent event){
        log.info("[{}] Current: {}º", event.componentId(), event.value());
        kafkaTemplate.send("sensor.temperature.updated", event.componentId().toString(), event);
    }
}
