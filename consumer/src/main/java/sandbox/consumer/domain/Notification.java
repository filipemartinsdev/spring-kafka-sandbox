package sandbox.consumer.domain;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity @Table(name = "notification")
@Data @AllArgsConstructor @NoArgsConstructor
public class Notification {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @NotNull
    private String text;

    @NotNull
    private Instant timestamp;

    @NotNull @Column(name = "created_at")
    private Instant createdAt = Instant.now();
}
