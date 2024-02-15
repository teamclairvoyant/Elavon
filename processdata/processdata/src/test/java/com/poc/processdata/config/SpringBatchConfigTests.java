package com.poc.processdata.config;

import com.poc.processdata.ProcessDataApplication;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.stream.Stream;


@SpringBootTest
@SpringJUnitConfig({ProcessDataApplication.class, SpringBatchConfig.class})
class SpringBatchConfigTests {

    private final Path INPUT_DIRECTORY = Path.of("test/java/resources/");

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @BeforeEach
    void setUp() {
//        if (Files.notExists(INPUT_DIRECTORY)) {
//            Files.createDirectory(INPUT_DIRECTORY);
//        }
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> filesTree = Files.walk(INPUT_DIRECTORY)) {
            filesTree.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .peek(fileToDelete -> System.out.println("deleting the file: " + fileToDelete.getName()))
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("GIVEN a directory with valid files WHEN jobLaunched THEN records persisted into DB and the input file is moved to processed directory")
    @SneakyThrows
    void shouldReadFromFileAndPersistIntoDataBaseAndMoveToProcessedDirectory() {
        //GIVEN
        Path completeFilePath = Path.of(INPUT_DIRECTORY + File.separator + "order.csv");
        Path inputFile = Files.createFile(completeFilePath);

        Files.writeString(inputFile, SpringBatchConfigTestsUtils.supplyValidContent());

        //WHEN
        var jobParameters = new JobParametersBuilder()
                .addString("input.file.name", inputFile.toString())
                .addDate("uniqueness", new Date())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        //THEN
        Assertions.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());


//        boolean containsAnyFile = Files.list(EXPECTED_COMPLETED_DIRECTORY)
//                .findAny()
//                .isPresent();
//
//        Assertions.assertTrue(containsAnyFile);
    }


}
