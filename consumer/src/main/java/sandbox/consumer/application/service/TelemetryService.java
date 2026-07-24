package sandbox.consumer.application.service;

import org.springframework.stereotype.Service;
import sandbox.consumer.application.dto.event.TemperatureAlertEvent;
import sandbox.consumer.domain.Notification;
import sandbox.consumer.infra.persistence.NotificationRepository;

@Service
public class TelemetryService {
    private final NotificationRepository notificationRepository;

    public TelemetryService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public void handleAlert(TemperatureAlertEvent event) {
        var notification = new Notification();
        notification.setText(event.text());
        notification.setTimestamp(event.timestamp());

        notificationRepository.save(notification);
    }
}
