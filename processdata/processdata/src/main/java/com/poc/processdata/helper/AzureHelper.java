package com.poc.processdata.helper;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.file.datalake.*;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@Component
@NoArgsConstructor
public class AzureHelper {

    @Value("${spring.azure.adls.containerName}")
    private String containerName;

    @Value("${spring.azure.adls.filePath}")
    private String adlsFilePath;

    @Value("${spring.batch.file.result}")
    private String resultPath;

    @Value("${spring.azure.adls.tenantId}")
    private  String tenant_id;

    @Value("${spring.azure.adls.clientId}")
    private  String client_id;

    @Value("${spring.azure.adls.clientSecret}")
    private  String client_sec;

    @Value("${spring.azure.adls.sasStringUrl}")
    private  String sas_url;


    public void pushToADLS() {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(client_id)
                .clientSecret(client_sec)
                .tenantId(tenant_id)
                .build();

        // Create DataLakeServiceClient
        DataLakeServiceClient serviceClient = new DataLakeServiceClientBuilder()
                .credential(clientSecretCredential)
                .endpoint(sas_url)
                .buildClient();
        DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(containerName);


        // Upload files from local folder to ADLS Gen2
        try {
            Files.list(Paths.get(resultPath)).forEach(filePath -> {
                String fileName = filePath.getFileName().toString();
                DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(String.valueOf(adlsFilePath));
                DataLakeFileClient fileClient = directoryClient.createFile(fileName, true);
                fileClient.uploadFromFile(filePath.toString(),true);
                log.info("File uploaded successfully: " + fileName);
                //System.out.println("Uploaded file: " + fileName);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
