package com.spring.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.spring.service.LoginDao;
import com.spring.util.GenerateToken;

//Logout Module

@RestController
public class LogoutController {

	 	@RequestMapping(value = "/logout", method = RequestMethod.GET)
	    public void submit(HttpServletRequest request, HttpServletResponse response) {
	 		
	 		
	 		    Cookie[] cookies = request.getCookies();
	 	        if(cookies != null){
	 		    for(int i = 0; i < cookies.length ; i++){
	 	            if(cookies[i].getName().equals("AUTHTOKEN")){
	 	            	cookies[i].setPath("/");
	 	                cookies[i].setMaxAge(0);
	 	                response.addCookie(cookies[i]);
	 	                break;
	 	            }
	 	        }
	 	     }       	    
	    }
	    
	}
