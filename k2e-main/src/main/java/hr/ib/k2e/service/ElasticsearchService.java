package hr.ib.k2e.service;

import hr.ib.k2e.dto.ESMessage;
import hr.ib.k2e.dto.MessagePrice;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Service used for processing data and inserting into Elasticsearch
 */

@Service
public class ElasticsearchService {

    /**
     * Elasticsearch index
     */
    protected static final String INDEX = "messageid";
    /**
     * Elasticsearch document
     */
    protected static final String DOCUMENT = "messageLog";
    /**
     * Elasticsearch painless script used for upserting data
     */
    protected static final String SCRIPT = "ctx._source.billing.add(params.billing)";
    /**
     * Elasticsearch scripting language
     */
    protected static final String SCRIPT_LANG = "painless";

    /**
     * Autowired instance of {@link BulkProcessor} used for collecting data and
     * bulk inserting into Elasticsearch
     */
    private final BulkProcessor bulkProcessor;

    public ElasticsearchService(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    /**
     * Adds an {@link UpdateRequest} to the {@link BulkProcessor} with the upsert option if document
     * with the given id doesn't exist.
     * @param message message which gets added to {@link BulkProcessor}
     */
    public void bulkPartialUpsert(ESMessage message) {
        Map<String, Object> params = createParamsForUpdate(message.getMessagePrice());
        IndexRequest indexRequest = null;
        try {
            indexRequest = createIndexRequest(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        UpdateRequest updateRequest = createUpdateRequest(message.getMessageLog().getId(), indexRequest, params);
        bulkProcessor.add(updateRequest);
    }

    /**
     * Creates an {@link UpdateRequest} with an instance of {@link IndexRequest} if a document with the
     * given id doesn't exist
     * @param id id of the document which will get upserted
     * @param indexRequest instance of the {@link IndexRequest} class which inserts the document if it doesn't exist
     * @param params a {@link Map} containing only the part which updates the document if it exists
     * @return an instance of {@link UpdateRequest} to be added to {@link BulkProcessor}
     */
    public UpdateRequest createUpdateRequest(String id, IndexRequest indexRequest, Map<String, Object> params) {
        return new UpdateRequest(INDEX, DOCUMENT, id)
                .script(new Script(ScriptType.INLINE, SCRIPT_LANG, SCRIPT, params))
                .upsert(indexRequest);
    }

    /**
     * @param message object which gets inserted
     * @return {@link IndexRequest} instance with the given data
     * @throws IOException
     */
    public IndexRequest createIndexRequest(ESMessage message) throws IOException {
        return new IndexRequest(INDEX, DOCUMENT, message.getMessageLog().getId()).source(buildJson(message));
    }

    /**
     * Creates a json with the given data which matches the Elasticsearch document
     * @param message data with which the json is created
     * @return a json object to get inserted into Elasticsearch
     * @throws IOException
     */
    protected XContentBuilder buildJson(ESMessage message) throws IOException {
        return jsonBuilder()
                .startObject()
                .field("id", message.getMessageLog().getId())
                .field("networkId", message.getMessageLog().getNetworkId())
                .field("status", message.getMessageLog().getStatus())
                .field("isFinal", message.getMessageLog().isFinal())
                .field("date", message.getMessageLog().getDate())
                .field("billing")
                .startArray()
                .startObject()
                .field("id", message.getMessagePrice().getId())
                .field("accountId", message.getMessagePrice().getAccountId())
                .field("messageLogId", message.getMessagePrice().getMessageLogId())
                .field("price", message.getMessagePrice().getPrice())
                .field("isFinal", message.getMessagePrice().isFinal())
                .endObject()
                .endArray()
                .endObject();
    }

    /**
     * Creates a {@link Map} which will update the document if exists
     * @param price data with which a document will get updated
     * @return a {@link Map} with the given data in a required format
     */
    public Map<String, Object> createParamsForUpdate(MessagePrice price) {
        Map<String, Object> nestedMapForUpdate = new HashMap<>();
        nestedMapForUpdate.put("id", price.getId());
        nestedMapForUpdate.put("accountId", price.getAccountId());
        nestedMapForUpdate.put("messageLogId", price.getMessageLogId());
        nestedMapForUpdate.put("price", price.getPrice());
        nestedMapForUpdate.put("isFinal", price.isFinal());

        Map<String, Object> params = new HashMap<>();
        params.put("billing", nestedMapForUpdate);

        return params;
    }

}
