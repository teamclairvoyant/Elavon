package com.consumer.restgraphql.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.client.GraphQlClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
public class GraphQLConsumer<T> {

    @Autowired
    private GraphQlClient graphQlClient;

    public List<T> getEntityList(String document, Class<T> type, String path) {
        Mono<List<T>> entityList = graphQlClient.document(document).retrieve(path).toEntityList(type);
        return entityList.block();
    }

    public T getEntity(String document, Class<T> type, String path) {
        Mono<T> entity = graphQlClient.document(document).retrieve(path).toEntity(type);
        return entity.block();
    }
}
