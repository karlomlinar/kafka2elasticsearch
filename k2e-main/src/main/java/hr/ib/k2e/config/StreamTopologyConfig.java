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

@Configuration
public class StreamTopologyConfig {

    private final MessageConverter messageConverter = new MessageConverter();
    private final ElasticsearchService elasticsearchService;
    private static final long SECONDS_IN_FIVE_DAYS = 60*60*4*5;

    public StreamTopologyConfig(ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }

    @Value("${kafka.log-topic}")
    public String logTopic;

    @Value("${kafka.price-topic}")
    public String priceTopic;

    @Bean
    public Topology streamTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> priceStream = builder.stream(priceTopic);
        KTable<String, String> logStream = builder.table(logTopic);

        priceStream.join(logStream,
                (priceValue, logValue) -> new ESMessage(messageConverter.convertToMessageLog(logValue), messageConverter.convertToMessagePrice(priceValue)))
                .filter((key, value) -> LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - value.getMessageLog().getDate() < SECONDS_IN_FIVE_DAYS)
                .foreach((key, value) -> elasticsearchService.bulkPartialUpsert(value));
        return builder.build();
    }

}
