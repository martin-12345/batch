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

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.classify.Classifier;
import org.springframework.stereotype.Component;

@Component
public class MyClassifier implements Classifier<Person, ItemWriter< ? super Person>> {
    FileWriterFactory<Person> fact;

    public MyClassifier(FileWriterFactory<Person> writerFactory) {
        this.fact = writerFactory;
    }

    @Override
    public ItemWriter<? super Person> classify(Person person) {

        FlatFileItemWriter<Person> w;
        w = fact.getWriter(person.getFilename());
        w.open(new ExecutionContext());
        return w;
    }
}
