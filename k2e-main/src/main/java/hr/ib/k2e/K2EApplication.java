package hr.ib.k2e;

import hr.ib.k2e.config.StreamTopologyConfig;
import hr.ib.k2e.service.ElasticsearchService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@SpringBootApplication
public class K2EApplication {

    public static void main(String[] args) {
        SpringApplication.run(K2EApplication.class, args);
    }

    public K2EApplication(@Qualifier("streamProperties") Properties streamProperties, @Qualifier("streamTopology")Topology topology) {
        this.streamProperties = streamProperties;
        this.topology = topology;
    }

    private final Properties streamProperties;
    private final Topology topology;


    @Bean
    CommandLineRunner commandLineRunner() {
        return (String... args) -> {
            KafkaStreams streams = new KafkaStreams(topology, streamProperties);
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        };
    }
}
