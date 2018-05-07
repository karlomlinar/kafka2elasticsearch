package hr.ib.k2e

import com.fasterxml.jackson.databind.ObjectMapper
import hr.ib.k2e.config.ElasticsearchTransportClient
import hr.ib.k2e.dto.MessageLog
import hr.ib.k2e.dto.MessagePrice
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification
import static org.awaitility.Awaitility.*;

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

@SpringBootTest
class K2EApplicationE2ESpecification extends Specification {

    @Autowired
    K2EApplication application

    @Autowired
    TransportClient client

    @Autowired
    KafkaTemplate kafkaTemplate

    ObjectMapper objectMapper = new ObjectMapper()
    String logTopic = "message_log"
    String priceTopic = "message_price"
    String index = "messageid"
    String document = "messageLog"
    String id = "E2ETestId"

    MessageLog log = new MessageLog("E2ETestId", 4, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))
    MessageLog log2 = new MessageLog("njl", 8, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))

    MessagePrice price = new MessagePrice("abc", 1, "E2ETestId", 18, false)
    MessagePrice price2 = new MessagePrice("htz", 9, "kon", 8, false)

    def setup() {
        client.delete(new DeleteRequest(index, document, id))
    }
    def cleanup() {
        client.delete(new DeleteRequest(index, document, id))
    }

    def 'Verify document exists after pushing messages to kafka'() {
        when: 'messages are pushed to kafka'
            kafkaSend(logTopic, log.getId(), objectMapper.writeValueAsString(log))
            kafkaSend(logTopic, log2.getId(), objectMapper.writeValueAsString(log2))
            kafkaSend(priceTopic, price.getMessageLogId(), objectMapper.writeValueAsString(price))
            kafkaSend(priceTopic, price2.getMessageLogId(), objectMapper.writeValueAsString(price2))
            //Thread.sleep(2000)

        then: 'document with a correct id should be inserted into kafka'
            //client.get(new GetRequest(index, document, id)).get(25, TimeUnit.SECONDS).exists
            await().atMost(5, TimeUnit.SECONDS).until{ client.get(new GetRequest(index, document, id)).get().exists}

    }

    void kafkaSend(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value);
    }
}
