package com.poc.processdata.config.linemapper;

import org.springframework.batch.item.file.LineMapper;

public class CustomFileLineMapper implements LineMapper<FileLine> {

    @Override
    public FileLine mapLine(String line, int lineNumber) throws Exception {
        FileLine fileLine = new FileLine();
        fileLine.setLineData(line);
        return fileLine;
    }
}
