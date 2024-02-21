package com.poc.processdata.helper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.sql.Timestamp;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchHelper {

    @Value("${spring.batch.data.fieldsToBeTokenized}")
    private String fieldsToBeTokenized;

    @Value("${spring.batch.file.uuidColumns}")
    private String uuidColumns;

    @Value("${spring.batch.file.headerColumns}")
    private List<String> headerColumns;

//    @Value("${spring.batch.file.filePath}")
//    private String filePath;

    @Value("${spring.batch.file.decryptedDirectoryPath}")
    private String decryptedDirectoryPath;
    @Value("${spring.batch.file.ALGORITHM}")
    private String algorithm;
    @Value("${spring.batch.file.TRANSFORMATION}")
    private String transformation;

    @Value("${spring.batch.file.SECRET_KEY}")
    private String secretKey;

    private final RestTemplate restTemplate;

    /*
   Tokenize the specified fields in the JSONObject using an external service
    */
    public void tokenizeData(JSONObject responseJsonObject) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String[] fieldsToBeTokenizedArray = fieldsToBeTokenized.split(",");
        for (String fieldToTokenize : fieldsToBeTokenizedArray) {
            HttpEntity<String> httpEntity = new HttpEntity<>(responseJsonObject.get(fieldToTokenize).toString(), headers);
            String tokenizedValue = restTemplate.postForObject("http://localhost:8080/cryptoapp/tokenize", httpEntity, String.class);
            responseJsonObject.put(fieldToTokenize, tokenizedValue);
        }
    }

    public void tokenizeDataMultiple(JSONObject responseJsonObject) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String[] fieldsToBeTokenizedArray = fieldsToBeTokenized.split(",");
        for (String fieldToTokenize : fieldsToBeTokenizedArray) {
            HttpEntity<String> httpEntity = new HttpEntity<>(responseJsonObject.get(fieldToTokenize).toString(), headers);
            String tokenizedValue = restTemplate.postForObject("http://localhost:8080/cryptoapp/tokenize", httpEntity, String.class);
            responseJsonObject.put(fieldToTokenize, tokenizedValue);
        }
    }

    /*
    Create a unique record ID by concatenating values from specified UUID columns and timestamp
     */
    public JSONObject addRecordId(JSONObject response) {
        String[] uuidCols = uuidColumns.split(",");
        StringBuilder sb = new StringBuilder();
        for (String uuIdCol : uuidCols) {
            sb.append(response.get(uuIdCol)).append("_");
        }
        sb.append(new Timestamp(System.currentTimeMillis()));
        response.put("record_id", sb.toString());
        return response;
    }

    /*
    Convert comma-separated data into a JSONObject using header columns as keys
    */
    public JSONObject convertToJSON(String item) {
        String[] data = item.split(",");

        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < headerColumns.size() - 1; i++) {
            jsonObject.put(headerColumns.get(i), data[i]);
        }
        return jsonObject;
    }

    public void decrypt(File inputFile) {
        try {
            SecretKey secretKeySpec = new SecretKeySpec(this.secretKey.getBytes(), algorithm);
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
            log.info("files reading");

            File outputFile = new File(decryptedDirectoryPath + File.separator + inputFile.getName());
            // Perform operations with inputFile and outputFile as needed
            try (InputStream inputStream = new FileInputStream(inputFile);
                 OutputStream outputStream = new FileOutputStream(outputFile);
                 CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, cipher);
            ) {
                byte[] buffer = new byte[1024];
                int bytesRead = inputStream.read(buffer);
                while (bytesRead >= 0) {
                    cipherOutputStream.write(buffer, 0, bytesRead);
                    bytesRead = inputStream.read(buffer);
                }
            }

        } catch (Exception e) {
            log.error("Error encrypting/decrypting file:", e);
        }

    }

}
