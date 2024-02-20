package com.poc.processdata.config.listener;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class StepItemWriteListener implements ItemWriteListener<JSONObject> {

    @Value("${spring.batch.data.fieldsToBeTokenized}")
    private List<String> fieldsToBeTokenized;

    @Override
    public void beforeWrite(List<? extends JSONObject> items) {
        Map<String, List<String>> fieldsToBeTokenizedValue = new HashMap<>();
        items.forEach(jsonObject -> fieldsToBeTokenized.forEach(fieldName -> {
            List<String> value = null;
            if (fieldsToBeTokenizedValue.containsKey(fieldName)) {
                value = fieldsToBeTokenizedValue.get(fieldName);
                value.add(jsonObject.getString(fieldName));
            } else {
                value = new ArrayList<>();
                value.add(jsonObject.getString(fieldName));
            }
            fieldsToBeTokenizedValue.put(fieldName, value);
        }));
        log.info("======================STEP LISTENER==================");
        fieldsToBeTokenizedValue.forEach((key, value) -> {
            value.forEach(val -> log.info("step listener===============" + key + ":" + value));
        });

    }

    @Override
    public void afterWrite(List<? extends JSONObject> items) {
        // implementation not required as of now
    }

    @Override
    public void onWriteError(Exception exception, List<? extends JSONObject> items) {
        // implementation not required as of now
    }
}
