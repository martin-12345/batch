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

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class FileWriterFactory<T> {

    @Value("${output.dir:/tmp}")
    String location;

    Map<String, FlatFileItemWriter<T>> writers = new HashMap<>();

    FlatFileItemWriter<T> getWriter(String name) {
        File file= new File(name);
        String fileName = file.getName();

        FlatFileItemWriter<T> f;
        f = writers.get(fileName);
        if (f == null) {
            f = new FlatFileItemWriter<>();
            f.setResource(new FileSystemResource(location+"/"+fileName + ".out"));
            f.setAppendAllowed(true);
            f.setLineAggregator(new DelimitedLineAggregator<T>() {
                {
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<T>() {
                        {
                            setNames(new String[]{"firstName", "lastName", "value"});
                        }
                    });
                }

            });
            writers.put(fileName, f);
        }
        return f;
    }
}
