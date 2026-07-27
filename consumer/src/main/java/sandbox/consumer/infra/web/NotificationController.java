package sandbox.consumer.infra.web;

import io.github.responsekit.core.PagedResponse;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import sandbox.consumer.application.dto.NotificationResponse;
import sandbox.consumer.application.service.NotificationService;

@RestController @RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping
    public ResponseEntity<PagedResponse<NotificationResponse>> getAll(Pageable pageable){
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(notificationService.getAll(pageable));
    }
}
