package sandbox.consumer.application.service;

import org.springframework.stereotype.Service;
import sandbox.consumer.application.dto.event.NewTemperatureEvent;

import java.util.List;

@Service
public class TelemetryService {
    public void handleNewTemperature(NewTemperatureEvent event){

    }
}
