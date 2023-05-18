package com.martin;

public class Person {

    private String lastName;
    private String firstName;
    private String filename;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    private String value;


    public Person() {
    }

    public Person(String firstName, String lastName, String filename) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.filename = filename;
    }


    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "firstName: " + firstName + ", lastName: " + lastName;
    }

    public String getFilename() {
        return filename;
    }


}
