package com.consumer.restgraphql.controller;

import com.consumer.restgraphql.dto.PersonDTO;
import com.consumer.restgraphql.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/person")
public class PersonController {

    @Autowired
    private PersonService personService;

    @GetMapping
    public List<PersonDTO> getAllPersons() {
        return personService.getAllPersons();
    }

    @GetMapping("/{id}")
    public PersonDTO getPersonById(@PathVariable String id) {
        return personService.getPersonById(id);
    }
}
