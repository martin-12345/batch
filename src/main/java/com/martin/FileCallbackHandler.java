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
import org.springframework.batch.core.*;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.LineCallbackHandler;

import java.io.File;

public class FileCallbackHandler implements LineCallbackHandler , StepExecutionListener  {
    private static final Logger logger = LoggerFactory.getLogger(FileCallbackHandler.class);
    private final String outputLocation;

    private ExecutionContext c;

    public FileCallbackHandler(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    @Override
    public void handleLine(String line) {
        if(line.startsWith("email:")){
            String emailAddr = line.trim().substring(6);
            c.put("email", emailAddr);
        }
        c.put("header", line);
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        c = stepExecution.getExecutionContext();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        String fileName = (String) c.get("outputFile");
        String email = (String) c.get("email");

        if(stepExecution.getExitStatus().equals(ExitStatus.COMPLETED) && email!=null) {

            logger.info("Send email to {} saying file {} is ready.", email, outputLocation+ File.separator+fileName);
        }

        if(email == null) {
            logger.error("No header/email address in file {}", fileName);
        }

        return ExitStatus.COMPLETED;
    }
}
