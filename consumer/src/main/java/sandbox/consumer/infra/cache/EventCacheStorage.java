package sandbox.consumer.infra.cache;

import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;

import java.util.UUID;

@Component
public class EventCacheStorage {
    private static final String CACHE_NAME = "temperature-alerts";

    @Cacheable(value = CACHE_NAME, key = "#componentId")
    public TemperatureAlertEvent get(UUID componentId){
        return null;
    }

    @CachePut(value = CACHE_NAME, key = "#event.componentId()")
    public TemperatureAlertEvent put(TemperatureAlertEvent event){
        return event;
    }
}
