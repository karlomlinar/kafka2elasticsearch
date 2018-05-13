package hr.ib.k2e.config;

import hr.ib.k2e.dto.ESMessage;
import hr.ib.k2e.service.ElasticsearchService;
import hr.ib.k2e.util.MessageConverter;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Configuration class for Kafka streams topology
 */

@Configuration
public class StreamTopologyConfig {

    /**
     * Class containing methods which convert {@link String} messages from Kafka topics into POJOs
     */
    private final MessageConverter messageConverter = new MessageConverter();

    /**
     * Service accumulating data to bulk insert into Elasticsearch
     */
    private final ElasticsearchService elasticsearchService;

    /**
     * Seconds in five days
     */
    private static final long SECONDS_IN_FIVE_DAYS = 60*60*4*5;

    public StreamTopologyConfig(ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }

    /**
     * MessageLog Kafka topic
     */
    @Value("${kafka.log-topic}")
    public String logTopic;

    /**
     * MessagePrice Kafka topic
     */
    @Value("${kafka.price-topic}")
    public String priceTopic;

    /**
     *  Creates a {@link Topology} object which is the computational
     *  logic of a Kafka Streams applications
     *
     * @return {@link Topology} object which will be passed into a new
     * {@link org.apache.kafka.streams.KafkaStreams} instance
     */

    @Bean
    public Topology streamTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        // KStream instance of the message_price topic
        KStream<String, String> priceStream = builder.stream(priceTopic);
        // KTable instance of the message_log topic - topic should be compact
        KTable<String, String> logStream = builder.table(logTopic);

        priceStream.join(logStream,
                // joins the stream and the table based on the message key and returns a new object containing converted message values
                (priceValue, logValue) -> new ESMessage(messageConverter.convertToMessageLog(logValue), messageConverter.convertToMessagePrice(priceValue)))

                // filters through messages to only allow joined messages where message from the topic price_log must not be 5 days older than the
                // creation of the message from the topic message_log which has the same key
                .filter((key, value) -> LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - value.getMessageLog().getDate() < SECONDS_IN_FIVE_DAYS)

                // adds the message to BulkProcessor for further inserting into Elasticsearch
                .foreach((key, value) -> elasticsearchService.bulkPartialUpsert(value));
        return builder.build();
    }

}
