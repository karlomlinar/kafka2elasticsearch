package hr.ib.k2e.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Class for connecting to Elasticsearch
 */
@Component
public class ElasticsearchTransportClient {

    /**
     * Elasticsearch host
     */
    @Value("${elasticsearch.host}")
    String esHost;

    /**
     * Elasticsearch port
     */
    @Value("${elasticsearch.port}")
    Integer esPort;

    /**
     * @throws UnknownHostException
     * @return  A new instance of {@link TransportClient} with the given Elasticsearch host and port
     */
    @Bean
    TransportClient transportClient() throws UnknownHostException {
        return new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(esHost), esPort));
    }
}
