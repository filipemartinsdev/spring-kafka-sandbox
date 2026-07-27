package sandbox.consumer.application.service;

import io.github.responsekit.core.PagedResponse;
import io.github.responsekit.spring.PagedResponseFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import sandbox.consumer.application.dto.NotificationResponse;
import sandbox.consumer.domain.Notification;
import sandbox.consumer.infra.persistence.NotificationRepository;

@Service
public class NotificationService {
    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public PagedResponse<NotificationResponse> getAll(Pageable pageable){
        return PagedResponseFactory.fromPage(
                notificationRepository.findAll(pageable),
                this::toResponse
        );
    }

    private NotificationResponse toResponse(Notification entity){
        return new NotificationResponse(entity.getId(), entity.getTitle(), entity.getText(), entity.getCreatedAt());
    }
}
