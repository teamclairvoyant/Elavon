package com.demo.graphql.controller;

import com.demo.graphql.dto.PersonDTO;
import com.demo.graphql.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

@Controller
public class PersonGQLController {

    @Autowired
    private PersonService personService;

    @QueryMapping("person")
    public List<PersonDTO> getAllPersons() {
        return personService.getAll();
    }

    @QueryMapping("personById")
    public PersonDTO getPersonById(@Argument String id) {
        return personService.getById(id);
    }

}
