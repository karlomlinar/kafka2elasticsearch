package hr.ib.k2e.messageproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hr.ib.k2e.dto.MessageLog;
import hr.ib.k2e.dto.MessagePrice;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@SpringBootApplication
public class MessageProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MessageProducerApplication.class, args);
    }

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MessageProducerApplication(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        objectMapper = new ObjectMapper();
    }

    @Value("${kafka.log-topic}")
    String logTopic;

    @Value("${kafka.price-topic}")
    String priceTopic;

    @Bean
    CommandLineRunner commandLineRunner() {
        return args -> {
            sendLogs();
            sendPrices();
        };
    }

    private void sendLogs() throws JsonProcessingException {
        MessageLog log = new MessageLog("nova", 4, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        MessageLog log2 = new MessageLog("njl", 8, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        kafkaSend(logTopic, log.getId(), objectMapper.writeValueAsString(log));
        kafkaSend(logTopic, log2.getId(), objectMapper.writeValueAsString(log2));
    }

    private void sendPrices() throws JsonProcessingException {
        MessagePrice price = new MessagePrice("abc", 1, "nova", 18, false);
        MessagePrice price2 = new MessagePrice("htz", 9, "kon", 8, false);
        kafkaSend(priceTopic, price.getMessageLogId(), objectMapper.writeValueAsString(price));
        kafkaSend(priceTopic, price2.getMessageLogId(), objectMapper.writeValueAsString(price2));
    }

    private void kafkaSend(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value);
    }
}
