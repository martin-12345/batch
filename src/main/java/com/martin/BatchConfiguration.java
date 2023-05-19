package com.martin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	private FlatFileItemReader<Person> personItemReader;
	@Autowired
	private FlatFileItemWriter<Person> personItemWriter;
	@Autowired
	private ResourcePatternResolver resourcePatternResolver;

	@Value("${output.dir:/tmp}")
	String location;
	@Value("${input.dir://home/martin/test-workspace/parallel-file-processor/src/main/resources}")
	String inputLocation;
	@Value("${filename.pattern:/*.csv}" )
	String namePattern;

	@Bean("partitioner")
	@StepScope
	public CustomMultiResourcePartitioner partitioner() {
		CustomMultiResourcePartitioner partitioner
				= new CustomMultiResourcePartitioner();
		ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Resource[] resources;
		try {
			resources = resolver.getResources("file://"+inputLocation+namePattern);
		} catch (IOException e) {
			throw new RuntimeException("I/O problems when resolving"
					+ " the input file pattern.", e);
		}
		partitioner.setResources(resources);
		return partitioner;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(masterStep())
				.end()
				.build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.<Person, Person>chunk(10)
				.processor(processor())
				.writer(personItemWriter)
				.reader(personItemReader)
				.build();
	}

	@Bean
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(10);
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setQueueCapacity(10);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	@Bean
	@Qualifier("masterStep")
	public Step masterStep() {
		return stepBuilderFactory.get("masterStep")
				.partitioner("step1", partitioner())
				.step(step1())
				.taskExecutor(taskExecutor())
				.build();
	}

	@Bean
	@StepScope
	@Qualifier("personItemReader")
	@DependsOn("partitioner")
	public FlatFileItemReader<Person> personItemReader(@Value("#{stepExecutionContext[inputFile]}") String filename) {
		log.info("In Reader "+filename);
		FlatFileItemReader<Person> r = new FlatFileItemReader<>();
		r.setResource(new FileSystemResource(filename));
		r.setLineMapper((line, lineNumber) -> {
			String[] parts = line.split(",");
			return new Person(parts[0], parts[1]);
		});
		return r;
	}

	@Bean
	@StepScope
	@Qualifier("personItemWriter")
	@DependsOn("partitioner")
	public FlatFileItemWriter<Person> personItemWriter(@Value("#{stepExecutionContext[outputFile]}") String filename) {
		log.info("In writer "+filename);

		FlatFileItemWriter<Person> f = new FlatFileItemWriter<>();
		f.setResource(new FileSystemResource(location+"/"+filename));
		f.setAppendAllowed(true);

		f.setLineAggregator(new DelimitedLineAggregator<Person>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
					{
						setNames(new String[]{"firstName", "lastName", "value"});
					}
				});
			}
		});
		return f;
	}
}
