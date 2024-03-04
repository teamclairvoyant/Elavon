package com.demo.graphql.repository;

import com.demo.graphql.domain.Person;
import com.demo.graphql.utility.DataUtility;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PersonRepo {


    List<Person> persons = DataUtility.getPersons();

    public List<Person> getAll() {

        return persons;
    }

}
