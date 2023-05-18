/*
 *   Copyright (c) 2023 Martin Newstead.  All Rights Reserved.
 *
 *   The author makes no representations or warranties about the suitability of the
 *   software, either express or implied, including but not limited to the
 *   implied warranties of merchantability, fitness for a particular
 *   purpose, or non-infringement. The author shall not be liable for any damages
 *   suffered by licensee as a result of using, modifying or distributing
 *   this software or its derivatives.
 */
package com.martin;

import org.springframework.batch.item.file.LineMapper;

public class PersonMapper implements LineMapper<Person> {
    String filename;

    public PersonMapper(String filename) {
        this.filename = filename;
    }

    @Override
    public Person mapLine(String line, int lineNumber) throws Exception {
       String[] parts = line.split(",") ;
        return new Person(parts[0], parts[1], filename);
    }
}
