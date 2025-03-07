package com.bfr.vergiftungsregisterbackend.gizadaptererfurt;

import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.proecess.FallErfurtProcessor;
import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.read.FallErfurt;
import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.read.FallErfurtCsvMapper;
import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.write.DatabaseUtils;
import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.write.FallNormalisiert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
@EnableBatchProcessing
@PropertySource("classpath:db.properties")
public class FallErfurtJobConfiguration {

    @Value("classpath:daten_eva_clean_20250124.csv")
    private Resource gizdaten;

    private static final Logger LOGGER = LoggerFactory.getLogger(FallErfurtJobConfiguration.class);

    @Bean
    @Qualifier("fallErfurtJob")
    public AbstractJob fallErfurtJob(JobRepository jobRepository,
                                    @Qualifier("fallErfurtStep") Step fallErfurtStep) {
        return (AbstractJob) new JobBuilder("fallErfurtJob", jobRepository)
                .start(fallErfurtStep)
                .build();
    }

    @Bean
    @Qualifier("fallErfurtStep")
    public Step fallErfurtStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                               @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        return new StepBuilder("gizErfurt", jobRepository)
                // Reading in chunks of size 1, item-by-item
                .<FallErfurt, FallNormalisiert>chunk(1, transactionManager)
                // Reading from text file supplying mapper behavior
                .reader(new FlatFileItemReaderBuilder<FallErfurt>()
                        .name("gizErfurtReader")
                        .resource(gizdaten)
                        .lineMapper(new FallErfurtCsvMapper())
                        .build())
                .processor(new FallErfurtProcessor())
                .writer(new JdbcBatchItemWriterBuilder<FallNormalisiert>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter(DatabaseUtils.UPDATE_FALL_DATEN_NORMALISIERT_SETTER)
                        .sql(DatabaseUtils.constructUpdateFallNormalisiertQuery(FallNormalisiert.FALL_NORMALISIERT_TABLE_NAME))
                        .build())
                .listener(beforeStepLoggerListener())
                .build();
    }


    // Step execution listener that logs information about step and environment (thread) right before the start of the execution
    private static StepExecutionListener beforeStepLoggerListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                int partitionCount = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_COUNT, -1);
                int partitionIndex = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_INDEX, -1);
                if (partitionIndex == -1 || partitionCount == -1) {
                    LOGGER.info("Calculation step is about to start handling all session action records");
                } else {
                    String threadName = Thread.currentThread().getName();
                    LOGGER.info("Calculation step is about to start handling partition " + partitionIndex
                            + " out of total " + partitionCount + " partitions in the thread -> " + threadName);
                }
            }
        };
    }

    /* ******************************** Spring Batch Utilities are defined below ********************************** */

    /**
     * Since we would like to launch jobs asynchronously, we would like to create async job launcher,
     * since out-of-the-box Spring Batch job launcher is synchronous
     */
    @Bean
    @Qualifier("asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }

    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link EnableBatchProcessing} is not able
     * to auto-create data source bean, so it's defined explicitly here
     */
    @Bean
    public DataSource dataSource(@Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    // Transaction manager for source data source, to control boundaries of storing the data in Postgresql
    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(sourceDataSource);
        return transactionManager;
    }

    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is not able
     * do detect whether the database should be initialized on start. So, we explicitly override it with
     * supplying {@link BatchProperties} bean defined below. In the configuration with correct naming and
     * directory setup, 'spring.batch.initialize-schema' property should make things work magically
     */
    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is not able
     * do detect whether the database should be initialized on start. So, we explicitly define the
     * {@link BatchProperties} bean with auto-wiring the configured value. In the configuration with correct
     * naming and directory setup, 'spring.batch.initialize-schema' property should make things work magically
     */
    @Bean
    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
