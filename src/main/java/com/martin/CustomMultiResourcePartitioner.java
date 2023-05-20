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


import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CustomMultiResourcePartitioner implements Partitioner {

    private static final String DEFAULT_IN_KEY_NAME = "inputFile";
    private static final String DEFAULT_OUT_KEY_NAME = "outputFile";

    private static final String PARTITION_KEY = "partition";

    private Resource[] resources = new Resource[0];

    /**
     * The resources to assign to each partition. In Spring configuration you
     * can use a pattern to select multiple resources.
     *
     * @param resources the resources to use
     */
    public void setResources(Resource[] resources) {
        this.resources = resources;
    }

    /**
     * Assign the filename of each of the injected resources to an
     * {@link ExecutionContext}.
     *
     * @see Partitioner#partition(int)
     */
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int i = 0;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            Assert.state(resource.exists(), "Resource does not exist: " + resource);

            File file;
            try {
                file = Paths.get(resource.getURI()).toFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String absolutePath = file.getAbsolutePath();

            /*
            Stores the absolute path/name of the input file in the context but just the filename of the output
            file. The location and any additional suffix is added by the FileWriter
             */
            context.put(DEFAULT_IN_KEY_NAME, absolutePath);
            context.putString(DEFAULT_OUT_KEY_NAME, filename(resource.getFilename()));

            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }

    private String filename(String filename) {
        return filename.replaceAll("\\.csv$","-out.csv" );
    }
}
