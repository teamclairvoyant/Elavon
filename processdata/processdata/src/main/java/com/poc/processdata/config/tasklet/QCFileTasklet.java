package com.poc.processdata.config.tasklet;

import com.poc.processdata.helper.QCFileHelper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class QCFileTasklet implements Tasklet {


    private final QCFileHelper qcFileHelper;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        qcFileHelper.createQCFiles();
        return RepeatStatus.FINISHED;
    }
}
