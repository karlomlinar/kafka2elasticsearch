package hr.ib.k2e.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static org.elasticsearch.client.RestClient.builder;

@Component
public class ElasticsearchConfig {

    @Value("${elasticsearch.username}")
    String username;

    @Value("${elasticsearch.password}")
    String password;

    @Value("${elasticsearch.url}")
    String url;

    @Value("${elasticsearch.port}")
    Integer port;

    @Value("${elasticsearch.scheme}")
    String scheme;

    @Bean
    RestClient restClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = builder(new HttpHost(url, port, scheme))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        builder.setMaxRetryTimeoutMillis(10000);
        return builder.build();
    }
}
