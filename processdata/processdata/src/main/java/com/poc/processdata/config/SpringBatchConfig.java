package com.poc.processdata.config;

import com.opencsv.CSVWriter;
import com.poc.processdata.config.listener.ItemWriteListener;
import com.poc.processdata.config.listener.SpringBatchListener;
import com.poc.processdata.helper.BatchHelper;
import io.micrometer.core.lang.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/*
Configuration class for Spring Batch, defining properties, and writing necessary components.
 */
@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringBatchConfig {

    private static final String FILE_DELIMITER = "\\";

    @Value("${spring.batch.file.decryptedFilePath}")
    private String decryptedFilePath;

    @Value("${spring.batch.file.headerColumns}")
    private String headerColumns;

    @Value("${spring.batch.file.result}")
    private String resultPath;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    private final SpringBatchListener springBatchListener;

    private final BatchHelper batchHelper;

    private final ItemWriteListener itemWriteListener;

    /*
    This will create a beam of Item reader and item reader reads data from source
    Created FlatFileItemReader instance and configured it
     */
    @Bean
    @StepScope
    public FlatFileItemReader<String> flatFileItemReader(@Value("#{stepExecutionContext[fileName]}") @Nullable String fileName) {
        log.info("reading files from reader");
        FlatFileItemReader<String> flatFileItemReader = new FlatFileItemReader<>();
        log.info("FILENAME===========reader" + fileName);
        FileSystemResource resource = new FileSystemResource(fileName.substring(5));
        flatFileItemReader.setResource(resource);

        flatFileItemReader.setLineMapper(new PassThroughLineMapper());
        flatFileItemReader.setLinesToSkip(1);
        return flatFileItemReader;
    }

    /*
    Process each item: log, print, convert to JSON, tokenize, and add record ID
     */
    @Bean
    @StepScope
    public ItemProcessor<String, JSONObject> itemProcessor() {
        return item -> {
            log.info("processing data");
            log.info(item);
            JSONObject jsonObject = batchHelper.convertToJSON(item);

            log.info("get json objects", jsonObject);

            batchHelper.tokenizeData(jsonObject);

            return batchHelper.addRecordId(jsonObject);
        };
    }

    /*
    Writing each item to a CSV file using specified header columns and
    Converting the item string to a JSONObject
    Extract data corresponding to header columns
    and Throw a runtime exception if an IO exception occurs
     */
    @Bean
    @StepScope
    public ItemWriter<JSONObject> itemWriter(@Value("#{stepExecutionContext[fileName]}") String filePath) {
        return items -> items.forEach(item ->
        {
            log.info((item.toString()), "-------------");
            log.info(filePath + "---------writer---------------");
            File file = new File(resultPath + FILE_DELIMITER + filePath.substring(filePath.lastIndexOf("/") + 1));
            String columns = headerColumns;
            String[] columnsArr = columns.split(",");
            String[] data = new String[columnsArr.length + 1];
            int i = 0;
            for (String key : columnsArr) {
                data[i] = item.get(key).toString();
                i++;
            }
            try (FileWriter outputFile = new FileWriter(file, true); CSVWriter writer = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)) {
                writer.writeNext(data);
            } catch (IOException e) {
                log.error("error while writing", e);
            }
        });
    }


    @Bean
    public Step cerateMasterStep() {
        return stepBuilderFactory.get("MasterStep")
                .partitioner("partition", createPartitioner())
                .step(step1())
                .gridSize(4)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Partitioner createPartitioner() {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            partitioner.setResources(resolver.getResources(decryptedFilePath));
        } catch (IOException e) {
            log.error("error in createPartitioner()", e);
        }
        return partitioner;

    }

    /*
    Configure and build a Step named "step1" to read, process, and write data in chunks
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, JSONObject>chunk(10)
                .listener(itemWriteListener)
                .reader(flatFileItemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter(null))
                .taskExecutor(taskExecutor()).throttleLimit(10)
                .build();
    }

    private TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(20);
        executor.setCorePoolSize(2);
        executor.afterPropertiesSet();
        return executor;
    }


    /*
    Create and return a new instance of the SpringBatchListener as a JobExecutionListener
     */

    /*
    Configure and build a Spring Batch job named "job" with a listener, incrementer, and a starting step
     */
    @Bean
    public Job job() {
        return jobBuilderFactory.get("job").listener(springBatchListener)
                .incrementer(new RunIdIncrementer())
                .flow(cerateMasterStep())
                .end()
                .build();
    }

}
