package org.example;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileToJsonConverter {

    public static void main(String[] args) {
        String inputFolderPath = "C:\\Users\\Kamal199261\\Documents\\csv_mul_dec_files";
        String outputFolderPath = "C:\\Users\\Kamal199261\\Documents\\json_files";

        try {
            File inputFolder = new File(inputFolderPath);
            File[] files = inputFolder.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        String fileName = file.getName();
                        String fileType = fileName.substring(fileName.lastIndexOf('.') + 1);
                        System.out.println(fileName);
                        System.out.println(fileType);
                        List<String> lines = FileUtils.readLines(file, "UTF-8");

                        JSONObject jsonObject = new JSONObject();
                        JSONArray jsonArray = new JSONArray();

                        String[] headers = lines.get(0).split(",");
                        jsonObject.put("headers", headers);

                        for (int i = 1; i < lines.size(); i++) {
                            String[] recordValues = lines.get(i).split(",");
                            JSONObject record = new JSONObject();
                            for (int j = 0; j < headers.length; j++) {
                                record.put(headers[j], recordValues[j]);
                            }
                            jsonArray.put(record);
                        }
                        jsonObject.put("records", jsonArray);

                        File outputFile = new File(outputFolderPath + "/" + fileName.substring(0,fileName.indexOf(".")) + "_" + fileType + ".json");
                        FileUtils.writeStringToFile(outputFile, jsonObject.toString(4), "UTF-8");
                    }
                }
                System.out.println("Conversion completed successfully.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

