package com.demo.graphql.utility;

import com.demo.graphql.domain.Person;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class DataUtility {

    public static List<Person> getPersons() {
        return Arrays.asList(new Person(UUID.randomUUID(), "ABC", "XYZ", new Date(), "category1"),
                new Person(UUID.randomUUID(), "ABC", "XYZ", new Date(), "category2"),
                new Person(UUID.randomUUID(), "fname1", "sname1", new Date(), "category3"),
                new Person(UUID.randomUUID(), "qwerty", "Qwerty", new Date(), "category4"),
                new Person(UUID.randomUUID(), "Test", "test", new Date(), "category5"),
                new Person(UUID.randomUUID(), "firstname", "lastname", new Date(), "category6"),
                new Person(UUID.randomUUID(), "NAME", "NAME", new Date(), "category7"),
                new Person(UUID.randomUUID(), "cdcdcd", "bmbmbm", new Date(), "category8")
        );
    }

}
