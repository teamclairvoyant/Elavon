package com.poc.processdata.config;

import com.opencsv.CSVWriter;
import com.poc.processdata.config.tasklet.PushToADLSTasklet;
import com.poc.processdata.config.tasklet.QCFileTasklet;
import com.poc.processdata.helper.BatchHelper;
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

    @Value("${spring.batch.file.filePath}")
    private String filePath;

    @Value("${spring.batch.file.decryptedDirectoryPath}")
    private String decryptedDirectoryPath;

    @Value("${spring.batch.file.headerColumns}")
    private String headerColumns;

    @Value("${spring.batch.file.result}")
    private String resultPath;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    private final BatchHelper batchHelper;

    private final TaskExecutor taskExecutor;

    private final QCFileTasklet qcFileTasklet;

    private final PushToADLSTasklet pushToADLSTasklet;

    /*
    This will create a beam of Item reader and item reader reads data from source
    Created FlatFileItemReader instance and configured it
     */
    @Bean
    @StepScope
    public FlatFileItemReader<String> flatFileItemReader(@Value("#{stepExecutionContext[fileName]}") String fileName) {
        log.info("reading files from reader");
        FlatFileItemReader<String> flatFileItemReader = new FlatFileItemReader<>();
        File file = new File(fileName.substring(5));
        batchHelper.decrypt(file);
//        List<String> dataList = batchHelper.decryptAsList(file);
//        System.out.println("list size=================="+dataList.size());
//        ListItemReader<String> itemReader = new ListItemReader<>(dataList);

        FileSystemResource resource = new FileSystemResource(decryptedDirectoryPath + File.separator + file.getName());
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setLineMapper(new PassThroughLineMapper());
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setSkippedLinesCallback(skippedLine -> {
            File resultFile = new File(resultPath + File.separator + file.getName());
            try (FileWriter outputFile = new FileWriter(resultFile); CSVWriter writer = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)) {
                writer.writeNext(headerColumns.split(","));
            } catch (IOException e) {
                log.error("error while writing header", e);
            }
        });
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

            return jsonObject;
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
        return items -> {
            batchHelper.tokenizeDataAndAddRecordId(items);
            items.forEach(item -> {
                File file = new File(resultPath + File.separator + filePath.substring(filePath.lastIndexOf("/") + 1));
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
        };
    }


    @Bean
    public Step readFilesMasterStep() {
        return stepBuilderFactory.get("readFilesMasterStep")
                .partitioner("readFilesPartitioner", readFilesPartitioner())
                .step(readAndProcessStep())
                .gridSize(500)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Partitioner readFilesPartitioner() {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            partitioner.setResources(resolver.getResources(filePath));
        } catch (IOException e) {
            log.error("error in createPartitioner()", e);
        }
        return partitioner;
    }

    /*
    Configure and build a Step named "step1" to read, process, and write data in chunks
     */
    @Bean
    public Step readAndProcessStep() {
        return stepBuilderFactory.get("readAndProcessStep")
                .<String, JSONObject>chunk(1000)
                .reader(flatFileItemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter(null))
                .taskExecutor(taskExecutor)
                .throttleLimit(8)
                .build();
    }

    @Bean
    public Step generateQCMasterStep() {
        return stepBuilderFactory.get("generateQCMasterStep")
                .partitioner("generateQCMasterStep", generateQCPartitioner())
                .step(generateQCFileTasklet())
                .gridSize(5)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public Partitioner generateQCPartitioner() {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            partitioner.setResources(resolver.getResources("file:" + resultPath + "/*.csv"));
        } catch (IOException e) {
            log.error("error in createPartitioner()", e);
        }
        return partitioner;
    }

    @Bean
    public Step generateQCFileTasklet() {
        return stepBuilderFactory.get("generateQCFileTasklet")
                .tasklet(qcFileTasklet)
                .build();
    }

    @Bean
    public Step pushResultsToADLSTasklet() {
        return stepBuilderFactory.get("pushResultsToADLSTasklet")
                .tasklet(pushToADLSTasklet)
                .build();
    }


    /*
    Create and return a new instance of the SpringBatchListener as a JobExecutionListener
     */

    /*
    Configure and build a Spring Batch job named "job" with a listener, incrementer, and a starting step
     */
    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .incrementer(new RunIdIncrementer())
                .flow(readFilesMasterStep())
                .next(generateQCMasterStep())
                .next(pushResultsToADLSTasklet())
                .end()
                .build();
    }

}
