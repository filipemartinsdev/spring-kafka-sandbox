package sandbox.consumer.infra.events;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import sandbox.consumer.application.dto.event.NewTemperatureEvent;
import sandbox.consumer.application.service.TelemetryService;

@Slf4j
@Component
public class EventConsumer {
    private final TelemetryService telemetryService;

    public EventConsumer(TelemetryService telemetryService) {
        this.telemetryService = telemetryService;
    }

    @KafkaListener(
            topics = "sensor.temperature.updated",
            groupId = "telemetry-service"
    )
    public void consume(NewTemperatureEvent event){
        log.info("Received temperature {}º", event.value());
        telemetryService.handleNewTemperature(event);
    }
}
