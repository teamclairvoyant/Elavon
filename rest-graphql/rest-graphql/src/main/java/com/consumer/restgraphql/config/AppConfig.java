package com.consumer.restgraphql.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.client.HttpGraphQlClient;
import org.springframework.web.reactive.function.client.WebClient;


@Configuration
public class AppConfig {

    @Bean
    public HttpGraphQlClient graphQLWebClient() {
        WebClient client = WebClient.builder()
                .baseUrl("http://localhost:8081/graphql/")
                .build();
        return HttpGraphQlClient.builder(client).build();
    }

}
