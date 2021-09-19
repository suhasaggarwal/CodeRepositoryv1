package com.spring.controller;
 
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Enumeration;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.loginResponse;
import com.spring.service.LoginDao;
import com.spring.service.UserDetailsDAO;
import com.spring.util.EncryptionModule;
import com.spring.util.GenerateToken;

//Login module - Sets Cookie for Autologin 

@RestController
public class LoginController {
  

	@RequestMapping(value = "/login", method = RequestMethod.POST,consumes="application/json",produces = MediaType.APPLICATION_JSON_VALUE)
            
   public  loginResponse  submit(@RequestBody LoginBean logininfo, HttpServletRequest request, HttpServletResponse response) {
        
		
		
  	if(LoginDao.validate(logininfo.getEmailid(),logininfo.getPassword())){
		
		
		long time = System.currentTimeMillis();
 		
    	
		 
		 String token = GenerateToken.getEncryptedToken(logininfo.getEmailid()+":"+time);
		 String usertype = UserDetailsDAO.GetUserType(logininfo.getEmailid());
			//  token = EncryptionModule.encrypt("1", "1 ",token);
				
			  try {
			token = URLEncoder.encode(token, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			  Cookie tokenCookie = new Cookie("AUTHTOKEN",
		                      token);
		 
		//	  cacheObject.putUserInfo(token,username);
			  Cookie tokenCookie1 = new Cookie("UserType",usertype);
			  

		      // Set expiry date after 6 Hrs for  cookies.
		      tokenCookie.setMaxAge(60*60*6); 
		      tokenCookie.setPath("/");
		      // Add both the cookies in the response header.
		     
		      tokenCookie1.setMaxAge(60*60*6); 
		      tokenCookie1.setPath("/");
		      
		      response.addCookie(tokenCookie);
		      response.addCookie(tokenCookie1);
    	      loginResponse response1 = new loginResponse(); 
    	      response1.setType("success");
    	      response1.setDescription("Authentication Verified");
    	      return response1;
    	
    } 
        
  	loginResponse response1 = new loginResponse(); 
    response1.setType("error");
    response1.setDescription("Username Password does not match");
	return response1;
}

     
    
}