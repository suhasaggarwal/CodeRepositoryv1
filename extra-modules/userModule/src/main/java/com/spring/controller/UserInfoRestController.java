package com.spring.controller;

import java.net.URLDecoder;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.User;
import com.spring.model.UserRole;
import com.spring.service.UserDao;
import com.spring.service.UserDetailsDAO;
import com.spring.util.EncryptionModule;

//Fetch details of registered user

@RestController
public class UserInfoRestController {

	
        //-------------------User Details API--------------------------------------------------------
		
		@RequestMapping(value = "/getuserdetails", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
		public ResponseEntity<User> GetUserDetails(HttpServletRequest request) {
        
		     String token;	
		     String userdetails;
		     String [] userinfo;
		     String emailId = null;
			 User user = new User();
		     Cookie[] cookies = request.getCookies();
	 	            if(cookies != null){
		     for(int i = 0; i < cookies.length ; i++){
	 	            if(cookies[i].getName().equals("AUTHTOKEN")){
                 //Fetch User details and return in json format
		              token = cookies[i].getValue();
		              userdetails=EncryptionModule.decrypt(null, null,URLDecoder.decode(token));
	 	              userinfo = userdetails.split(":");
	                  emailId = userinfo[0];
	 	            }
	 	        }
	 	      }
	 	            
	 	    if(emailId != null)
	 	    user=UserDetailsDAO.GetUserDetails(emailId);
			
	 	    if(user != null)
			return new ResponseEntity<User>(user, HttpStatus.OK);   
			else
			return new ResponseEntity<User>(HttpStatus.NOT_FOUND);
}      
}