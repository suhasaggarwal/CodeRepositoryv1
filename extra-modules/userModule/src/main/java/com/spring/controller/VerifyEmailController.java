package com.spring.controller;

import java.io.IOException;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.SaveUserResponse;
import com.spring.service.LoginDao;
import com.spring.service.TokenDAO;
import com.spring.service.UpdateUserDAO;

@RestController
public class VerifyEmailController {



	@RequestMapping(value = "/ConfirmRegistration/{token}", method = RequestMethod.GET)
    public void verifyemail(@PathVariable("token") String token, HttpServletResponse response) {
 		//Verify token from database, if token exist in database, user is authentic
		
		String status="false";
		SaveUserResponse response1 = new SaveUserResponse();
        
		
		if(TokenDAO.VerifyToken(token)){
 		status=UpdateUserDAO.updateUserLoginState("Confirmed", token);
 		
 		
 		
 		 if(status.equals("true")){
	        	
	        	
        	 response1.setType("success");
        	 response1.setDescription("User Details saved successfully");
        	 try {
				response.sendRedirect("http://cuberoot.com");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
   
        else{
        	 
        	
        	 response1.setType("error");
        	 response1.setDescription("Error in saving User Data");
        	 try {
				response.sendRedirect("http://errorpage");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
 		
 		
 		
    }




	}

}
