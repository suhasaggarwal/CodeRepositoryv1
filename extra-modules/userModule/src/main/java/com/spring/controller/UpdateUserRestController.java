package com.spring.controller;

import java.net.URLDecoder;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.UpdateUser;
import com.spring.model.UpdateUserResponse;
import com.spring.model.User;
import com.spring.service.UpdateUserDAO;
import com.spring.service.UserDetailsDAO;
import com.spring.util.EncryptionModule;


//Update Already registered Data

@RestController
public class UpdateUserRestController {


	@RequestMapping(value = "/updateuserdetails", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public UpdateUserResponse updateUserDetails(@RequestBody UpdateUser user,HttpServletRequest request) {
    
	     String token;	
	     String userdetails;
	     String [] userinfo;
	     String status;
	     String emailId = null;
	     String address = null;
	     String state = null;
	     String city = null;
	     String zipcode = null;
	     String phone = null;
	   
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
	        
 	       if(user != null)
 	       {
 	        address = user.getAddress();
	        
 	
 	 	    state = user.getState();
 	 	    
 	        city = user.getCity();
 	        
	 	   
 	        phone = user.getPhone();
 	        
	 	    
 		 	zipcode = user.getZipcode();
 	       
 	        emailId = user.getEmailid();
 	       
 	       }   
 	      
 	        status=UpdateUserDAO.updateUser(address, state, city, zipcode, phone, emailId);
		
 	       UpdateUserResponse response1 = new UpdateUserResponse();
 	        
 	        if(status.equals("true")){
 	        	response1.setType("success");
 	            response1.setDescription("Details Updated");
 	  			return response1;
 	  		
 	        }
 	        else{
 	        	
 	        	response1.setType("error");
 	        	response1.setDescription("Error Saving Details");
 	            return response1;
 	        }
 	        	
 	        


	}



}
