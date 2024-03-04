package com.demo.graphql.service;

import com.demo.graphql.dto.PersonDTO;
import com.demo.graphql.mapper.PersonMapper;
import com.demo.graphql.repository.PersonRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class PersonService {

    @Autowired
    private PersonRepo personRepo;

    @Autowired
    private PersonMapper personMapper;

    public List<PersonDTO> getAll() {
        return this.personMapper.map(this.personRepo.getAll());
    }

    public PersonDTO getById(String id) {
        return this.personMapper.map(
                this.personRepo.getAll()
                        .stream()
                        .filter(person -> person.getId().toString().equals(id))
                        .collect(Collectors.toList())
                        .get(0));
    }


}
