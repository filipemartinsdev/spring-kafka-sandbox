package sandbox.consumer.infra.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import sandbox.consumer.domain.Temperature;

import java.util.UUID;

public interface TemperatureRepository extends JpaRepository<Temperature, UUID> {
}
