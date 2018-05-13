package hr.ib.k2e.config;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Class for Elasticsearch {@link BulkProcessor} configuration
 */

@Component
public class BulkProcessorConfig {

    /**
     * Elasticsearch client
     */
    private final TransportClient transportClient;

    public BulkProcessorConfig(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    /**
     * Creates a new {@link BulkProcessor} and defines properties like <i>bulk actions</i> and <i>flush interval</i>
     * @return A configured {@link BulkProcessor} instance
     * @throws IOException
     */
    @Bean
    public BulkProcessor bulkProcessor() throws IOException {

        return BulkProcessor.builder(transportClient,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        System.out.println("STARTED BULKING");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        System.out.println(response.buildFailureMessage());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println(failure.getCause().toString());
                    }
                })
                // sets the maximum amount of data to insert at once
                .setBulkActions(1000)
                // sets the maximum size of data to insert
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                // sets the flush interval
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                // sets the number of concurrent requests
                .setConcurrentRequests(1)
                // sets the backoff policy in case of failure
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }
}
