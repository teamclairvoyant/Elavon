package com.demo.graphql.mapper;

import com.demo.graphql.domain.Person;
import com.demo.graphql.dto.PersonDTO;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class PersonMapper {

    public PersonDTO map(Person person) {
        PersonDTO personDTO = new PersonDTO();
        personDTO.setId(person.getId().toString());
        personDTO.setFirstName(person.getFirstName());
        personDTO.setLastName(person.getLastName());
        personDTO.setDob(person.getDob());
        personDTO.setCategory(person.getCategory());
        return personDTO;
    }

    public List<PersonDTO> map(List<Person> persons) {
        return persons.stream().map(this::map).collect(Collectors.toList());
    }

}
