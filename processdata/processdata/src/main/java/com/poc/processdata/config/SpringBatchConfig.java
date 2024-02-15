package com.poc.processdata.config;
import lombok.extern.slf4j.Slf4j;
import com.opencsv.CSVWriter;
import com.poc.processdata.config.listener.SpringBatchListener;
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
import org.springframework.beans.factory.annotation.Autowired;
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

@Slf4j
@Configuration
public class SpringBatchConfig {

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

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private RestTemplate restTemplate;


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
        File file = new File(resultPath + "\\" + resource.getFilename());
        try (FileWriter outputFile = new FileWriter(file); CSVWriter writer = new CSVWriter(outputFile)) {
            String[] header = headerColumns.split(",");
            writer.writeNext(header);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return flatFileItemReader;
    }

    @Bean
    @StepScope
    public ItemProcessor<String, String> itemProcessor() {
        return item -> {
            log.info("processing data");
            System.out.println(item);

            JSONObject jsonObject = convertToJSON(item);

            log.info("get json objects",jsonObject);

            tokenizeData(jsonObject);

            return addRecordId(jsonObject);
        };
    }

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


    private String decrypt(JSONObject jsonObject) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> httpEntity = new HttpEntity<>(jsonObject.toString(), headers);
        return restTemplate.postForObject("http://localhost:8080/cryptoapp/decrypt", httpEntity, String.class);
    }

    private JSONObject convertToJSON(String item) {
        String[] data = item.split(",");

        String[] columnsArr = headerColumns.split(",");

        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < columnsArr.length - 1; i++) {
            jsonObject.put(columnsArr[i], data[i]);
        }
        return jsonObject;
    }

    @Bean
    @StepScope
    public ItemWriter<String> itemWriter() {
        return items -> items.forEach(item ->
        {
            File file = new File(resultPath + "\\" + filePath.substring(filePath.lastIndexOf("\\")));
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
                throw new RuntimeException(e);
            }
        });
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(10)
                .reader(flatFileItemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new SpringBatchListener();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job").listener(jobExecutionListener())
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

}
