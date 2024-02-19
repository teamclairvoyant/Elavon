package com.poc.processdata.config.linemapper;

import org.jetbrains.annotations.NotNull;
import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

public class FileLine implements ResourceAware {

    private String lineData;

    private Resource resource;

    public Resource getResource() {
        return resource;
    }

    @Override
    public void setResource(@NotNull Resource resource) {
        this.resource = resource;
    }

    public String getLineData() {
        return lineData;
    }

    public void setLineData(String lineData) {
        this.lineData = lineData;
    }
}
