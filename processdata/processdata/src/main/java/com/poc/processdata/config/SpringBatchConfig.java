package com.poc.processdata.config;

import com.opencsv.CSVWriter;
import com.poc.processdata.config.listener.SpringBatchListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
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
@RequiredArgsConstructor
public class SpringBatchConfig {

    private static final String FILE_DELIMITER = "\\";

    @Value("${spring.batch.file.filePath}")
    private String filePath;

    @Value("${spring.batch.file.decryptedFilePath}")
    private String decryptedFilePath;

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

    /*
    This will create a beam of Item reader and item reader reads data from source
    Created FlatFileItemReader instance and configured it
     */
    @Bean
    @StepScope
    public FlatFileItemReader<String> flatFileItemReader() {
        log.info("reading files from reader");
        FlatFileItemReader<String> flatFileItemReader = new FlatFileItemReader<>();
        FileSystemResource resource = new FileSystemResource(decryptedFilePath);
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(new PassThroughLineMapper());
        File file = new File(resultPath + FILE_DELIMITER + resource.getFilename());
        try (FileWriter outputFile = new FileWriter(file); CSVWriter writer = new CSVWriter(outputFile)) {
            String[] header = headerColumns.split(",");
            writer.writeNext(header);
        } catch (IOException e) {
            log.error("error while writing headers", e);
        }

        return flatFileItemReader;
    }

    /*
    Process each item: log, print, convert to JSON, tokenize, and add record ID
     */
    @Bean
    @StepScope
    public ItemProcessor<String, String> itemProcessor() {
        return item -> {
            log.info("processing data");
            log.info(item);

            JSONObject jsonObject = convertToJSON(item);

            log.info("get json objects", jsonObject);

            tokenizeData(jsonObject);

            return addRecordId(jsonObject);
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
            sb.append(response.get(uuIdCol) + "_");
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
    public ItemWriter<String> itemWriter() {
        return items -> items.forEach(item ->
        {
            File file = new File(resultPath + FILE_DELIMITER + filePath.substring(filePath.lastIndexOf("\\")));
            JSONObject jsonObject = new JSONObject(item);
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

    /*
    Configure and build a Step named "step1" to read, process, and write data in chunks
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(10)
                .reader(flatFileItemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
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
        return jobBuilderFactory.get("job").listener(springBatchListener)
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

}
