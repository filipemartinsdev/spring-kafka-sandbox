package sandbox.consumer.infra.events;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.application.service.TelemetryService;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
public class EventConsumer {
    private final TelemetryService telemetryService;
    private final ObjectMapper objectMapper;

    public EventConsumer(TelemetryService telemetryService, ObjectMapper objectMapper) {
        this.telemetryService = telemetryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "sensor.temperature.alerts",
            groupId = "telemetry-service"
    )
    public void consume(String json){
        TemperatureAlertEvent event = objectMapper.readValue(json, TemperatureAlertEvent.class);
        log.info("Received alert {}º", event.text());

        telemetryService.handleAlert(event);
    }
}
