package com.poc.processdata.config;

import com.opencsv.CSVWriter;
import com.poc.processdata.config.linemapper.CustomFileLineMapper;
import com.poc.processdata.config.linemapper.FileLine;
import com.poc.processdata.config.listener.SpringBatchListener;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

/*
Configuration class for Spring Batch, defining properties, and writing necessary components.
 */
@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringBatchConfig {

    private static final String FILE_DELIMITER = "\\";

    @Value("${spring.batch.file.filePath}")
    private String filePath;

    @Value("${spring.batch.file.decryptedFilePath}")
    private String decryptedFilePath;

//    @Value("classpath:/decrypted/*.csv")
//    private Resource[] inputFiles;

    @Value("${spring.batch.file.headerColumns}")
    private String headerColumns;

    @Value("${spring.batch.file.uuidColumns}")
    private String uuidColumns;

    @Value("${spring.batch.file.result}")
    private String resultPath;

    @Value("${spring.batch.data.fieldsToBeTokenized}")
    private String fieldsToBeTokenized;


    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    private final RestTemplate restTemplate;

    private final SpringBatchListener springBatchListener;

//    @Bean
//    @StepScope
//    public MultiResourceItemReader<FileLine> multiResourceItemReader() {
//        MultiResourceItemReader<FileLine> resourceItemReader = new MultiResourceItemReader<>();
//        ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
//        try {
//            resourceItemReader.setResources(resourcePatternResolver.getResources(decryptedFilePath));
//        } catch (IOException e) {
//            log.error("error", e);
//        }
//        resourceItemReader.setDelegate(flatFileItemReader());
//        return resourceItemReader;
//    }


    /*
    This will create a beam of Item reader and item reader reads data from source
    Created FlatFileItemReader instance and configured it
     */
    @Bean
    @StepScope
    public FlatFileItemReader<FileLine> flatFileItemReader(@Value("#{stepExecutionContext[fileName]}") String fileName) {
        log.info("reading files from reader");
        FlatFileItemReader<FileLine> flatFileItemReader = new FlatFileItemReader<>();
        System.out.println("FILENAME===========" + fileName);
        FileSystemResource resource = new FileSystemResource(fileName.substring(5));
        flatFileItemReader.setResource(resource);
        CustomFileLineMapper lineMapper = new CustomFileLineMapper();
        lineMapper.setResource(resource);
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setLinesToSkip(1);
        return flatFileItemReader;
    }

    /*
    Process each item: log, print, convert to JSON, tokenize, and add record ID
     */
    @Bean
    @StepScope
    public ItemProcessor<FileLine, FileLine> itemProcessor() {
        return item -> {
            log.info("processing data");
            log.info(item.getLineData());
            JSONObject jsonObject = convertToJSON(item.getLineData());

            log.info("get json objects", jsonObject);

            tokenizeData(jsonObject);

            String recordIdAddedData = addRecordId(jsonObject);
            item.setLineData(recordIdAddedData);
            return item;
        };
    }

    /*
    Tokenize the specified fields in the JSONObject using an external service
     */
    private void tokenizeData(JSONObject responseJsonObject) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String[] fieldsToBeTokenizedArray = fieldsToBeTokenized.split(",");
        for (String fieldToTokenize : fieldsToBeTokenizedArray
        ) {
            HttpEntity<String> httpEntity = new HttpEntity<>(responseJsonObject.get(fieldToTokenize).toString(), headers);
            String tokenizedValue = restTemplate.postForObject("http://localhost:8080/cryptoapp/tokenize", httpEntity, String.class);
            responseJsonObject.put(fieldToTokenize, tokenizedValue);
        }
    }

    /*
    Create a unique record ID by concatenating values from specified UUID columns and timestamp
     */
    private String addRecordId(JSONObject response) {
        String[] uuidCols = uuidColumns.split(",");
        StringBuilder sb = new StringBuilder();
        for (String uuIdCol : uuidCols) {
            sb.append(response.get(uuIdCol)).append("_");
        }
        sb.append(new Timestamp(System.currentTimeMillis()));
        response.put("record_id", sb.toString());
        return response.toString();
    }


    /*
    Convert comma-separated data into a JSONObject using header columns as keys
     */
    private JSONObject convertToJSON(String item) {
        String[] data = item.split(",");

        String[] columnsArr = headerColumns.split(",");

        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < columnsArr.length - 1; i++) {
            jsonObject.put(columnsArr[i], data[i]);
        }
        return jsonObject;
    }

    /*
    Writing each item to a CSV file using specified header columns and
    Converting the item string to a JSONObject
    Extract data corresponding to header columns
    and Throw a runtime exception if an IO exception occurs
     */
    @Bean
    @StepScope
    public ItemWriter<FileLine> itemWriter() {
        return items -> items.forEach(item ->
        {
            log.info(item.getLineData(), "-------------");
            String filename = item.getResource().getFilename();
            log.info(filename+"---------------");
            File file = new File(resultPath + FILE_DELIMITER + filename);
            JSONObject jsonObject = new JSONObject(item.getLineData());
            String columns = headerColumns;
            String[] columnsArr = columns.split(",");
            String[] data = new String[columnsArr.length + 1];
            int i = 0;
            for (String key : columnsArr) {
                data[i] = jsonObject.get(key).toString();
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
            throw new RuntimeException(e);
        }
        return partitioner;

    }

    /*
    Configure and build a Step named "step1" to read, process, and write data in chunks
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<FileLine, FileLine>chunk(10)
                .reader(flatFileItemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter()).taskExecutor(taskExecutor()).throttleLimit(10)
                .build();
    }

    private TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(20);
        executor.setCorePoolSize(2);
        executor.afterPropertiesSet();
        executor.setThreadNamePrefix("GithubLookup-");
        executor.initialize();

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
