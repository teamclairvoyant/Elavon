package com.crpyto.demoapp.controller;

import com.crpyto.demoapp.service.CryptoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class CryptoController {

    @Autowired
    private CryptoService cryptoService;


    @PostMapping("/decrypt")
    public String decrypt(@RequestBody String dataToBeDecrypted) {
        return cryptoService.decryptJSONData(dataToBeDecrypted);
    }

}
