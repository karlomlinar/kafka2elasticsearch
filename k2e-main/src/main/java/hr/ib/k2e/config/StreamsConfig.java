package hr.ib.k2e.config;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class StreamsConfig {

    @Bean
    @Qualifier("streamProperties")
    Properties properties(@Value("${kafka.host}") String bootstrapServers, @Value("${kafka.port}") String bootstrapPort,
                          @Value("${kafka.application-id}") String applicationId) {

        Properties streamsProperties = new Properties();
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers + ":" + bootstrapPort);
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return streamsProperties;
    }
}