package com.poc.processdata.config.tasklet;

import com.poc.processdata.helper.AzureHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PushToADLSTasklet implements Tasklet {

    private final AzureHelper azureHelper;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Pushing to ADLS2");
        azureHelper.pushToADLS();
        return RepeatStatus.FINISHED;
    }
}
