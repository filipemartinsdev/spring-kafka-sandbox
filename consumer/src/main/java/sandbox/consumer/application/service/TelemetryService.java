package sandbox.consumer.application.service;

import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.domain.Notification;
import sandbox.consumer.infra.cache.EventCacheStorage;
import sandbox.consumer.infra.persistence.NotificationRepository;

import java.util.UUID;

@Slf4j
@Service
public class TelemetryService {
    private final NotificationRepository notificationRepository;
    private final EventCacheStorage eventCacheStorage;

    public TelemetryService(NotificationRepository notificationRepository, EventCacheStorage eventCacheStorage) {
        this.notificationRepository = notificationRepository;
        this.eventCacheStorage = eventCacheStorage;
    }

    @Transactional
    public void handleAlert(TemperatureAlertEvent event) {
        if (isRecentlyAlerted(event.componentId()))
            return;

        var notification = new Notification();
        notification.setTitle("[ALERT] Component temperature exceeded");
        notification.setText(event.text());
        notification.setTimestamp(event.timestamp());
        notificationRepository.save(notification);

        eventCacheStorage.put(event);

        log.info("Received alert: {}", event);
    }

    private boolean isRecentlyAlerted(UUID componentId){
        return eventCacheStorage.get(componentId) != null;
    }
}
