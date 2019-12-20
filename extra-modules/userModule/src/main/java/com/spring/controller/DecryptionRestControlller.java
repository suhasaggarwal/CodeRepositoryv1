package com.spring.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.UserIdentification;
import com.spring.util.EncryptionModule;


@RestController
public class DecryptionRestControlller {

	//-------------------User Details API--------------------------------------------------------
	
			@RequestMapping(value = "/decrypt/{token}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
			public String GetUserInfo(@PathVariable("token") String username) {

			//	UserIdentification userId = new UserIdentification();
				try {
					username=EncryptionModule.decrypt(null, null,URLDecoder.decode(username,"UTF-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			//	userId.setEmailId(username);
				
			    return username;
			
			}

}
