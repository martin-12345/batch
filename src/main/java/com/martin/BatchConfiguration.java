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
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
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

import java.io.File;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	private FlatFileItemReader<Person> personItemReader;
	@Autowired
	private FlatFileItemWriter<Person> personItemWriter;

	@Value("${output.dir:/tmp}")
	private String location;
	@Value("${input.dir:/home/martin/test-workspace/parallel-file-processor/src/main/resources}")
	private String inputLocation;
	@Value("${filename.pattern:*.csv}" )
	private String namePattern;

	private static final Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);

	/*
    This bean collects the files and passes then as Resources to the partitioner. The Partitioner
    is used in the MasterStep to invoke a sub-step in a new thread to process one file. Each step has its
    own copy of the Reader and Writer. Thus, multiple threads can process discreet files simultaneously.
     */
	@Bean("partitioner")
	@StepScope
	public CustomMultiResourcePartitioner partitioner() {
		CustomMultiResourcePartitioner partitioner
				= new CustomMultiResourcePartitioner();
		ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Resource[] resources;
		try {
			resources = resolver.getResources("file://"+inputLocation+ File.separator+namePattern);
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
	@StepScope
	public FileCallbackHandler headerLineCallback(){
		return new FileCallbackHandler(location);
	}

	@Bean
	public Job importUserJob(JobNotificationListener listener) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(initialStep())
				.next(masterStep())
				.build();
	}

	@Bean
	public Step initialStep(){
		return stepBuilderFactory.get("initialStep")
				.tasklet(fileDeletingTasklet())
				.build();
	}


	@Bean
	public FileDeletingTasklet fileDeletingTasklet() {
		FileDeletingTasklet tasklet = new FileDeletingTasklet();

		tasklet.setDirectoryResource(location, namePattern);

		return tasklet;
	}

	@Bean
	@Qualifier("masterStep")
	public Step masterStep() {
		return stepBuilderFactory.get("masterStep")
				.partitioner("subStep", partitioner())
				.step(subStep())
				.taskExecutor(taskExecutor())
				.build();
	}
	@Bean
	@Qualifier("subStep")
	public Step subStep() {

		return stepBuilderFactory.get("subStep")
				.<Person, Person>chunk(10)
				.reader(personItemReader)
				.processor(processor())
				.writer(personItemWriter)
				.listener(headerLineCallback())
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
	@StepScope
	@Qualifier("personItemReader")
	@DependsOn("partitioner")
	public FlatFileItemReader<Person> personItemReader(@Value("#{stepExecutionContext[inputFile]}") String filename) {

		return new FlatFileItemReaderBuilder<Person>()
				.name("personItemReader")
				.resource(new FileSystemResource(filename))
				.delimited()
				.names("firstName", "lastName")
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
					setTargetType(Person.class);
				}})
				.linesToSkip(1)
				.skippedLinesCallback(headerLineCallback())
				.build();
	}

	@Bean
	@StepScope
	@Qualifier("personItemWriter")
	@DependsOn("partitioner")
	public FlatFileItemWriter<Person> personItemWriter(@Value("#{stepExecutionContext[header]}") String header, @Value("#{stepExecutionContext[outputFile]}") String filename) {

		return new FlatFileItemWriterBuilder<Person>()
				.name("personItemWriter")
				.resource(new FileSystemResource(location + File.separator+ filename))
				.append(true)
				.headerCallback(outputHeaderCallback(header))
				.lineAggregator(new DelimitedLineAggregator<Person>() {
					/*
                    Gets passed an object, in this case a Person object and the LineAggregator extracts the attribute listed
                    in the setNames below (by calling the getters by means of the BeanWrapperFieldExtractor), aggregates the
                    values, separated by comma, the delimiter, and the FileWriter writes the line to the output file.
                     */
					{
						setDelimiter(",");
						setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
							{
								setNames(new String[]{"firstName", "lastName", "value"});
							}
						});
					}
				}).build();
	}

	public FlatFileHeaderCallback outputHeaderCallback(String header) {
		return new OutputHeaderCallback(header);
	}
}
