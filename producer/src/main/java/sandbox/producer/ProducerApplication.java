package sandbox.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import sandbox.producer.application.dto.Temperature;
import sandbox.producer.application.dto.event.NewTemperatureEvent;
import sandbox.producer.application.service.TemperatureEngine;
import sandbox.producer.infra.events.EventProducer;

@SpringBootApplication @EnableScheduling
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

	private final TemperatureEngine temperatureEngine;
	private final EventProducer eventProducer;

	public ProducerApplication(TemperatureEngine temperatureEngine, EventProducer eventProducer) {
		this.temperatureEngine = temperatureEngine;
		this.eventProducer = eventProducer;
	}

	@Scheduled(cron = "*/2 * * * * *")
	public void newTemperature(){
		Temperature temperature = temperatureEngine.next();

		eventProducer.produceNewTemperature(
				new NewTemperatureEvent(
						temperature.componentId(),
						temperature.value(),
						temperature.timestamp()
				)
		);
	}
}
