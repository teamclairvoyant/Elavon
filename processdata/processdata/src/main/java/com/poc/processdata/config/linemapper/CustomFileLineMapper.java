package com.poc.processdata.config.linemapper;

import org.springframework.batch.item.file.LineMapper;
import org.springframework.core.io.Resource;

public class CustomFileLineMapper implements LineMapper<FileLine> {

    private Resource resource;

    @Override
    public FileLine mapLine(String line, int lineNumber) throws Exception {
        FileLine fileLine = new FileLine();
        fileLine.setLineData(line);
        fileLine.setResource(this.resource);
        return fileLine;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
}
