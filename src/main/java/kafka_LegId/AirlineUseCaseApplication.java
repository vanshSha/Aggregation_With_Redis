package kafka_LegId;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class AirlineUseCaseApplication {

	public static void main(String[] args) {
		SpringApplication.run(AirlineUseCaseApplication.class, args);
	}

}
