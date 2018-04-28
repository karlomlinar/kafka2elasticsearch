package hr.ib.k2e.service

import groovy.json.JsonSlurper
import hr.ib.k2e.dto.ESMessage
import hr.ib.k2e.dto.MessageLog
import hr.ib.k2e.dto.MessagePrice
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.transport.TransportClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import spock.lang.Specification

@SpringBootTest
class ElasticsearchServiceIntegrationSpecification extends Specification {

    @Autowired
    ElasticsearchService elasticsearchService

    @Autowired
    BulkProcessor bulkProcessor

    @Autowired
    TransportClient client

    JsonSlurper jsonSlurper = new JsonSlurper()
    MessagePrice messagePrice = new MessagePrice("id", 32, "someid", 6, false)
    MessagePrice messagePrice2 = new MessagePrice("i2", 432, "someid", 5, true)
    MessageLog messageLog = new MessageLog("someid", 5, "ACCEPTED", false, 8)
    ESMessage esMessage = new ESMessage(messageLog, messagePrice)
    ESMessage esMessage2 = new ESMessage(messageLog, messagePrice2)

    def setup() {
        client.delete(new DeleteRequest("messageid", "messageLog", esMessage.messageLog.id))
    }
    def cleanup() {
        client.delete(new DeleteRequest("messageid", "messageLog", esMessage.messageLog.id))
    }

    def 'Successful insert to elastic test'() {

        when: 'a get request is invoked for an id not inserted'
            def result = client.get(new GetRequest(elasticsearchService.INDEX, elasticsearchService.DOCUMENT, messageLog.id))

        then: 'the result should not exist'
            !result.get().exists

        when: 'bulkPartialUpsert is invoked with an ESMessage object'
            elasticsearchService.bulkPartialUpsert(esMessage)
        and: 'is then flushed'
            bulkProcessor.flush()

        then: 'a document with the inserted id exists'
            client.get(new GetRequest(elasticsearchService.INDEX, elasticsearchService.DOCUMENT, messageLog.id)).get().exists
    }

    def 'Successful upsert to elastic test'() {

        when: "a document with the id doesn't exist"
            !client.get(new GetRequest(elasticsearchService.INDEX, elasticsearchService.DOCUMENT, messageLog.id)).get().exists
        and: 'a document is then inserted with the same id'
            elasticsearchService.bulkPartialUpsert(esMessage)
        and: 'another document is inserted where price messageLogId is the same as log id'
            elasticsearchService.bulkPartialUpsert(esMessage2)
            bulkProcessor.flush()

        then: 'after index refresh'
            client.admin().indices().prepareRefresh(elasticsearchService.INDEX).get()
        and: 'inserted document should have billing list of size 2'
            jsonSlurper.parseText(client.get(new GetRequest(elasticsearchService.INDEX, elasticsearchService.DOCUMENT, messageLog.id)).get().getSourceAsString()).billing.size == 2



    }

}
