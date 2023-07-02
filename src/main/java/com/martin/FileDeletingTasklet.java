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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileDeletingTasklet implements Tasklet {
    private static final Logger log = LoggerFactory.getLogger(FileDeletingTasklet.class);
    private String pattern;
    private String location;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try (DirectoryStream<Path> fileStream = Files.newDirectoryStream(
                Paths.get(location), pattern)) {
            fileStream.forEach(path -> {
                if(Files.isRegularFile(path)) {
                    try {
                        Files.delete(path);
                        log.debug("deleted {}", path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        return RepeatStatus.FINISHED;
    }

    public void setDirectoryResource(String location, String pattern) {
        this.location = location;
        this.pattern = pattern;
    }
}
