package com.poc.processdata.config.tasklet;

import com.poc.processdata.helper.QCFileHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@RequiredArgsConstructor
@Slf4j
public class QCFileTasklet implements Tasklet {


    private final QCFileHelper qcFileHelper;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Creating QC file");
        File processedFile = new File(chunkContext.getStepContext().getStepExecutionContext().get("fileName").toString().substring(5));
        qcFileHelper.createQCFile(processedFile);
        log.info("QC file created");
        return RepeatStatus.FINISHED;
    }
}
