package com.spring.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.Emailidbean;
import com.spring.model.SaveUserResponse;
import com.spring.model.UpdateUser;
import com.spring.model.UpdateUserResponse;
import com.spring.service.TokenDAO;
import com.spring.service.UpdateUserDAO;
import com.spring.util.GenerateToken;
import com.spring.util.SendEmail;

//Controller for Forgot Password module

@RestController
public class ForgotPasswordController {

	@RequestMapping(value = "/forgotpassword", method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_VALUE)
    
	public void verifyemail(@RequestBody Emailidbean email, HttpServletRequest request) {
 		//Verify token from database, if token exist in database, user is authentic
		
		String emailid = email.getEmailid();
		
	    String token = GenerateToken.getToken(emailid);
	    
	    token = token.substring(0, 10);
	    
	    String status = null;
	    
	    status = SendEmail.sendEmailForgotPassword(emailid,token);
	
	    UpdateUserResponse response = new UpdateUserResponse();
	    
	    
	    
	    if(status.equals("true")){
	    	
	    	UpdateUserDAO.updateUserPassword(token, emailid);
	    	response.setType("success");
       	    response.setDescription("Please Check your mail for password");
       	
       }
  
       else{
       	 
       	
       	 response.setType("error");
       	 response.setDescription("Error in Resetting paSSWORD");
       	
       }
	    
	
	  }
	
	}

















