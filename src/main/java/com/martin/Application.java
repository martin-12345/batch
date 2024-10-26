package com.martin;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements ApplicationRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();

        for (String v : args.getSourceArgs()) {
            String[] parts=v.split("=");
            String name = parts[0];
            String value = parts[1];

            jobParametersBuilder.addString(name, value);
        }

        JobExecution execution = jobLauncher.run(job, jobParametersBuilder.toJobParameters());
    }
}
