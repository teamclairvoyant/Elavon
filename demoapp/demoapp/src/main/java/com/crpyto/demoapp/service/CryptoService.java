package com.crpyto.demoapp.service;

public interface CryptoService {

    String tokenize(String dataToBeEncrypted);

    String decryptJSONData(String data);
}
