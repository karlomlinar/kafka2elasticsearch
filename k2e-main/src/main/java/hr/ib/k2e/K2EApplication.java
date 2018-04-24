package hr.ib.k2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import hr.ib.k2e.dto.ESMessage;
import hr.ib.k2e.dto.MessageLog;
import hr.ib.k2e.dto.MessagePrice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

@SpringBootApplication
public class K2EApplication {

    public static void main(String[] args) {
        SpringApplication.run(K2EApplication.class, args);
    }

    public K2EApplication(@Qualifier("streamProperties") Properties streamProperties) {
        this.streamProperties = streamProperties;
        objectMapper = new ObjectMapper();
    }

    private final ObjectMapper objectMapper;
    private final Properties streamProperties;

    private static final long SECONDS_IN_FIVE_DAYS = 60*60*24*5;

    @Value("${kafka.log-topic}")
    private String logTopic;

    @Value("${kafka.price-topic}")
    private String priceTopic;

    @Bean
    CommandLineRunner commandLineRunner() {
        return (String... args) -> {

            KStreamBuilder builder = new KStreamBuilder();
            KStream<String, String> priceStream = builder.stream(priceTopic);
            KTable<String, String> logStream = builder.table(logTopic);

            priceStream.join(logStream,
                    (priceValue, logValue) -> {
                        MessagePrice price = null;
                        MessageLog log = null;
                        try {
                            price = objectMapper.readValue(priceValue, MessagePrice.class);
                            log = objectMapper.readValue(logValue, MessageLog.class);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return new ESMessage(log.getId(), log.getNetworkId(), log.getStatus(), log.isFinal(), log.getDate(), price);
                    })
                    .filter((key, value) -> LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - value.getDate() < SECONDS_IN_FIVE_DAYS)
                    .mapValues(input -> {
                        System.out.println(input);
                        return input;
                    });
            KafkaStreams streams = new KafkaStreams(builder, streamProperties);
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        };
    }

}
