package com.consumer.restgraphql.service;

import com.consumer.restgraphql.dto.PersonDTO;

import java.util.List;

public interface PersonService {

    List<PersonDTO> getAllPersons();

    PersonDTO getPersonById(String id);
}
