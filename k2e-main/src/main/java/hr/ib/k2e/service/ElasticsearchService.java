package hr.ib.k2e.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hr.ib.k2e.dto.ESMessage;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ElasticsearchService {

    private static final String BULK_URL = "/messageid/messageLog/_bulk";
    private static String ELASTICSEARCH_METADATA = "{\"update\":{\"_index\":\"messageid\",\"_type\":\"messageLog\",\"_id\":%1$s}}";
    private static String ELASTICSEARCH_SOURCE_DATA =
            "{  \n" +
                    "   \"script\":{  \n" +
                    "      \"source\":\"ctx._source.billing.add(params.billing)\",\n" +
                    "      \"lang\":\"painless\",\n" +
                    "      \"params\":{  \n" +
                    "%1$s" +
                    "         }\n" +
                    "      }\n" +
                    "   },\n" +
                    "   \"upsert\":{  \n" +
                    "%2$s" +
                    "   }\n" +
                    "}";


    private final ObjectMapper objectMapper;
    private final RestClient restClient;

    public ElasticsearchService(RestClient restClient) {
        this.restClient = restClient;
        objectMapper = new ObjectMapper();
    }

    public void bulkPartialUpsert(List<ESMessage> messages) {
        try {
            restClient.performRequest("POST", BULK_URL, Collections.emptyMap(), new NStringEntity(formBulkInsertData(messages), ContentType.APPLICATION_JSON));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private String formBulkInsertData(List<ESMessage> messages) {
        return messages.stream().map(message -> {
            StringBuilder stringBuilder = new StringBuilder();
            try {
                stringBuilder
                        .append(String.format(ELASTICSEARCH_METADATA, message.getMessageLog().getId()))
                        .append("\n")
                        .append(String.format(ELASTICSEARCH_SOURCE_DATA, objectMapper.writeValueAsString(message.getMessagePrice()), objectMapper.writeValueAsString(message.getMessageLog())))
                        .append("\n");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return stringBuilder.toString();
        }).collect(Collectors.joining());
    }
}
