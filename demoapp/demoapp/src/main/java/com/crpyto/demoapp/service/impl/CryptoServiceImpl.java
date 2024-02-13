package com.crpyto.demoapp.service.impl;

import com.crpyto.demoapp.service.CryptoService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Set;

@Service
public class CryptoServiceImpl implements CryptoService {

    @Autowired
    private SecureRandom secureRandom;

    @Autowired
    private Base64.Encoder base64Encoder;

    @Override
    public String tokenize(String dataToBeTokenized) {
        System.out.println("dataToBeTokenized"+dataToBeTokenized);
        if (null != dataToBeTokenized) {
            byte[] dataToBeTokenizedBytes = new byte[dataToBeTokenized.getBytes().length];
            secureRandom.nextBytes(dataToBeTokenizedBytes);
            return base64Encoder.encodeToString(dataToBeTokenizedBytes);
        }
        return null;
    }

    @Override
    public String decryptJSONData(String data) {
        JSONObject jsonObject = new JSONObject(data);
        JSONObject responseJSON = new JSONObject();
        Set<String> jsonKeys = jsonObject.keySet();
        jsonKeys.forEach(key -> {
            String decryptedData = decryptJSONKey(jsonObject.getString(key));
            responseJSON.put(key, decryptedData);
        });
        return responseJSON.toString();
    }

    private String decryptJSONKey(String value) {
        String secretKey = "MySecretKey12345";
        byte[] decodedKey = Base64.getDecoder().decode(secretKey);
        String decryptedData = null;
        try {
            Cipher cipher = Cipher.getInstance("AES");
            SecretKey originalKey = new SecretKeySpec(Arrays.copyOf(decodedKey, 16), "AES");
            cipher.init(Cipher.DECRYPT_MODE, originalKey);
            byte[] cipherText = cipher.doFinal(Base64.getDecoder().decode(value));
//            System.out.println("secKey: " + secretKey);
//            System.out.println("originalKey: " + originalKey);
            decryptedData = new String(cipherText);
//            System.out.println("DecryptedData: " + decryptedData);
            return decryptedData;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return decryptedData;
    }
}
