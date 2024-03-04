package com.consumer.restgraphql.service.impl;

import com.consumer.restgraphql.consumer.GraphQLConsumer;
import com.consumer.restgraphql.dto.PersonDTO;
import com.consumer.restgraphql.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class PersonServiceImpl implements PersonService {

    @Autowired
    private GraphQLConsumer<PersonDTO> graphQLConsumer;

    @Override
    public List<PersonDTO> getAllPersons() {
        String document = "{\n" +
                "       person {\n" +
                "           id\n" +
                "           firstName\n" +
//                "           lastName\n" +
                "           category\n" +
                "       }\n" +
                "}";
        return graphQLConsumer.getEntityList(document, PersonDTO.class, "person");
    }

    @Override
    public PersonDTO getPersonById(String id) {
        String document = "{\n" +
                "       personById(id:\"" + id + "\") {\n" +
                "           id\n" +
                "           firstName\n" +
                "           lastName\n" +
                "           category\n" +
                "       }\n" +
                "}";
        return graphQLConsumer.getEntity(document, PersonDTO.class, "personById");
    }
}
