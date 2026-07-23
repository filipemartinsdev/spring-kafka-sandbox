package sandbox.consumer.domain;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity @Table(name = "temperature")
@AllArgsConstructor @NoArgsConstructor @Data
public class Temperature {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @NotNull
    @Column(name = "component_id")
    private UUID componentId;

    @NotNull
    private Integer value;

    @NotNull
    private Instant timestamp = Instant.now();
}








