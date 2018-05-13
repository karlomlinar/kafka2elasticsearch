package hr.ib.k2e.config;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Configuration class for Kafka streams properties
 */

@Configuration
public class StreamsConfig {

    /**
     * Returns a {@link Properties} object containing Kafka host and port used for connecting to Kafka
     * and key and value {@link Serdes} for message
     * serialization and deserialization.
     * Also contains an {@code application.id} property which allows multiple instances
     * of this application to be in the same consumer group.
     *
     * @param bootstrapServers Kafka host
     * @param bootstrapPort Kafka port
     * @param applicationId unique application id
     * @return {@link Properties} object containing necessary Kafka properties
     */


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