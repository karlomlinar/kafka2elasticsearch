package hr.ib.k2e.service;

import hr.ib.k2e.dto.ESMessage;
import hr.ib.k2e.dto.MessagePrice;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class ElasticsearchService {

    private static final String INDEX = "messageid";
    private static final String DOCUMENT = "messageLog";
    private static final String SCRIPT = "ctx._source.billing.add(params.billing)";
    private static final String SCRIPT_LANG = "painless";

    private final BulkProcessor bulkProcessor;

    public ElasticsearchService(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

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

    private UpdateRequest createUpdateRequest(String id, IndexRequest indexRequest, Map<String, Object> params) {
        return  new UpdateRequest(INDEX, DOCUMENT, id)
                .script(new Script(ScriptType.INLINE, SCRIPT_LANG, SCRIPT, params))
                .upsert(indexRequest);
    }

    private IndexRequest createIndexRequest(ESMessage message) throws IOException {
        return new IndexRequest(INDEX, DOCUMENT, message.getMessageLog().getId()).source(
                jsonBuilder()
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
                        .endObject());
    }

    private Map<String, Object> createParamsForUpdate(MessagePrice price) {
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
