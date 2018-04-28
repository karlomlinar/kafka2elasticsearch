package hr.ib.k2e.service

import hr.ib.k2e.dto.ESMessage
import hr.ib.k2e.dto.MessageLog
import hr.ib.k2e.dto.MessagePrice
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import spock.lang.Specification

class ElasticsearchServiceSpecification extends Specification {

    ElasticsearchService elasticsearchService
    BulkProcessor bulkProcessor = Mock()
    MessagePrice messagePrice = new MessagePrice("id", 32, "5", 6, false)
    MessageLog messageLog = new MessageLog("5", 5, "ACCEPTED", false, 8)
    ESMessage esMessage = new ESMessage(messageLog, messagePrice)

    def setup() {
        elasticsearchService = Spy(ElasticsearchService, constructorArgs: [bulkProcessor])
    }

    def 'createParamsForUpdate test'() {

        when: 'createParamsForUpdate is invoked with an object of MessagePrice'
            def result = elasticsearchService.createParamsForUpdate(messagePrice)

        then: 'method result should of instance Map'
            result instanceof Map
        and: "result should contain map with 'billing' as key"
            def mapResult = result.get("billing")
        and: 'it should contain the given data'
            mapResult == [accountId:messagePrice.accountId, messageLogId:messagePrice.messageLogId, price:messagePrice.price, id:messagePrice.id, isFinal:messagePrice.final]
    }

    def 'buildJson test'() {

        when: 'buildJson method is invoked with an ESMessage object'
            def result = elasticsearchService.buildJson(esMessage)

        then: 'result should be of instance XContentBuilder'
            result instanceof XContentBuilder
        and: 'string representation should be equal to an elasticsearch document in json format with given data'
            result.string() ==
                '''{"id":"5","networkId":5,"status":"ACCEPTED","isFinal":false,"date":8,"billing":[{"id":"id","accountId":32,"messageLogId":"5","price":6,"isFinal":false}]}'''

    }

    def 'createIndexRequest test'() {

        when: 'createIndexRequest is invoked with an ESMessage object'
            def result = elasticsearchService.createIndexRequest(esMessage)

        then: 'result is of type IndexRequest'
            result instanceof IndexRequest
        and: 'index request data is corresponding with given data'
            result.id == esMessage.messageLog.id
            result.type == elasticsearchService.DOCUMENT
            result.index == elasticsearchService.INDEX
    }

    def 'createUpdateRequest test'() {
        given: 'empty params'
            Map<String, Object> dummyMap = new HashMap<>()
        and: 'a mocked index request'
            def indexRequest = Mock(IndexRequest)

        when: 'createUpdateRequest is invoked with the correct data'
            def result = elasticsearchService.createUpdateRequest(esMessage.getMessageLog().getId(), indexRequest, dummyMap)

        then: 'result is of type UpdateRequest'
            result instanceof UpdateRequest
        and: 'update request data is corresponding with given data'
            result.id == esMessage.messageLog.id
            result.script == new Script(ScriptType.INLINE, elasticsearchService.SCRIPT_LANG, elasticsearchService.SCRIPT, dummyMap)
            result.upsertRequest == indexRequest
    }

    def 'bulkPartialUpsert test'() {

        when: 'bulkPartialUpsert is invoked with an ESMessage object'
            elasticsearchService.bulkPartialUpsert(esMessage)

        then: 'params for upsert are created'
            1 * elasticsearchService.createParamsForUpdate(esMessage.messagePrice)
        and: 'index request is created with the given message'
            1 * elasticsearchService.createIndexRequest(esMessage)
        and: 'update request is created with the previously created data'
            1 * elasticsearchService.createUpdateRequest(esMessage.messageLog.id, _ as IndexRequest, _ as HashMap)
        and: 'UpdateRequest object is added to the bulkProcessor'
            1 * bulkProcessor.add(_ as UpdateRequest)
    }


}
