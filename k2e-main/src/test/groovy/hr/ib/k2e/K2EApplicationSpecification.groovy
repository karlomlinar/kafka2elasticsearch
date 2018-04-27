package hr.ib.k2e

import com.fasterxml.jackson.databind.ObjectMapper
import hr.ib.k2e.config.BulkProcessorConfig
import hr.ib.k2e.config.StreamTopologyConfig
import hr.ib.k2e.dto.ESMessage
import hr.ib.k2e.dto.MessageLog
import hr.ib.k2e.dto.MessagePrice
import hr.ib.k2e.service.ElasticsearchService
import hr.ib.k2e.util.MessageConverter
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.ZoneOffset

class K2EApplicationSpecification extends Specification {

    TopologyTestDriver testDriver
    ElasticsearchService elasticsearchService = Mock()
    MessageConverter converter = new MessageConverter()
    ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer())
    ObjectMapper objectMapper = new ObjectMapper()
    StreamTopologyConfig streamTopologyConfig
    String logTopic = "message_log"
    String priceTopic = "message_price"

    def setup() {
        streamTopologyConfig = Spy(StreamTopologyConfig, constructorArgs: [elasticsearchService])
        streamTopologyConfig.priceTopic = priceTopic
        streamTopologyConfig.logTopic = logTopic
        Properties config = new Properties()
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(streamTopologyConfig.streamTopology(), config)
    }

    def 'Kafka topology test'() {
        given: 'records of type MessageLog and MessagePrice to push where only two records have the same key and are in the same time frame(5 days)'
            MessageLog logJoin = new MessageLog("test", 5, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))
            MessagePrice priceJoin = new MessagePrice("test2", 5, "test", 7, false)

            MessageLog logNotJoin = new MessageLog("fail", 5, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))
            MessagePrice priceNotJoin = new MessagePrice("test2", 5, "FAIL", 7, false)

            MessageLog logJoinOutOfTimeFrame = new MessageLog("test", 5, "COMPLETED", false, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - 60*60*4*5)
            MessagePrice priceJoinOutOfTimeFrame = new MessagePrice("test2", 5, "test", 7, false)

        when: 'given records are pushed'
            //these will be joined because they have the same key
            testDriver.pipeInput(recordFactory.create(logTopic, logJoin.id, objectMapper.writeValueAsString(logJoin)))
            testDriver.pipeInput(recordFactory.create(priceTopic, priceJoin.messageLogId, objectMapper.writeValueAsString(priceJoin)))

            //these won't be joined because they don't have the same key
            testDriver.pipeInput(recordFactory.create(logTopic, logNotJoin.id, objectMapper.writeValueAsString(logNotJoin)))
            testDriver.pipeInput(recordFactory.create(priceTopic, priceNotJoin.messageLogId, objectMapper.writeValueAsString(priceNotJoin)))
            
            //these will be joined, but won't be processed because log topic was processed 5 days ago
            testDriver.pipeInput(recordFactory.create(logTopic, logJoinOutOfTimeFrame.id, objectMapper.writeValueAsString(logJoinOutOfTimeFrame)))
            testDriver.pipeInput(recordFactory.create(priceTopic, priceJoinOutOfTimeFrame.messageLogId, objectMapper.writeValueAsString(priceJoinOutOfTimeFrame)))

        then: 'invoke elasticsearchService once'
            1 * elasticsearchService.bulkPartialUpsert(_)
    }

    def cleanup() {
        // This has to be done so the test could pass, otherwise it fails with
        // DirectoryNotEmptyException because Kafka locks a temp file it needs to delete
        // https://github.com/apache/kafka/pull/4702
        try {
            testDriver.close()
        } catch (StreamsException exception) {
        }
    }

}
