package com.spring.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.spring.model.SaveUserResponse;
import com.spring.model.User;
import com.spring.model.loginResponse;
import com.spring.service.DeleteEmailDAO;
import com.spring.service.SaveUserDAO;
import com.spring.service.UpdateUserDAO;
import com.spring.service.UserDetailsDAO;
import com.spring.util.GenerateToken;
import com.spring.util.SendEmail;

//Sign up Module with Confirmation Mail - User only gets registered after verifying email

@RestController
public class SaveuserInfo {

	//-------------------User Details API------------------------------------------------------
	/*
	  @RequestMapping(value = "/saveuserdata/{fullname}/{emailname}/{username}/{password}/{advertiser}/{publisher}/{companyname}/{website_url}/{address}/{state}/{city}/{phone}/{zipcode}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
			public ResponseEntity<String> GetUserInfo(@PathVariable("fullname") String fullname,@PathVariable("emailname") String emailname,@PathVariable("username") String username,@PathVariable("password") String password,@PathVariable("advertiser") String advertiser,@PathVariable("publisher") String publisher,@PathVariable("companyname") String companyname,@PathVariable("website_url") String website_url,@PathVariable("address") String address,@PathVariable("state") String state,@PathVariable("city") String city,@PathVariable("phone") String phone,@PathVariable("zipcode") String zipcode) {
                String status;
				status=SaveUserDAO.saveUser(fullname,emailname,username,password,advertiser,publisher,companyname,website_url,address,state,city,phone,zipcode);
			    return new ResponseEntity<String>(status, HttpStatus.OK);
			
			}
		*/	

	            
	
	
	
	           @RequestMapping(value = "/adduser", method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_VALUE)
               
                
			    public SaveUserResponse submit(@RequestBody User user, HttpServletRequest request ) {
			      
			     
			        
			        /*
			        model.addAttribute("fullname", user.getFullName());
			        model.addAttribute("emailname", user.getEmailName());
			        model.addAttribute("username", user.getUsername());
			        model.addAttribute("password", user.getPassword());
			        model.addAttribute("advertiser", user.getAdvertiser());
			        model.addAttribute("publisher", user.getPublisher());
			        model.addAttribute("companyname", user.getCompanyName());
			        model.addAttribute("website_url", user.getWebsiteUrl());
			        model.addAttribute("address", user.getAddress());
			        model.addAttribute("state", user.getState());
			        model.addAttribute("city", user.getCity());
			        model.addAttribute("phone", user.getPhone());
			        model.addAttribute("zipcode", user.getZipcode());
			        */
			        String monthlyvisitors = null;
			        String firstname = user.getFirstname();
			        String lastname = user.getLastname();
			        String username = user.getUsername();
			        String emailid = user.getEmail();
			        String password = user.getPassword();
			        String usertype = user.getUsertype();
			        if(usertype.equals("publisher"))
			        monthlyvisitors = user.getMonthly_visitor();	
			        String companyname = user.getCompanyname();
			        String websiteurl = user.getWebsiteurl();
			        String address = user.getAddress();
			        String state = user.getState();
			        String city = user.getCity();
			        String phone = user.getPhone();
			        String zipcode = user.getZipcode();
			        String status1 = "false";
			       
			        String token = GenerateToken.getToken(emailid);
			        
			        User user1 = null;

					SaveUserResponse response = new SaveUserResponse();
			        
			        user1=UserDetailsDAO.GetUserDetails(emailid);
			        
			        if(user1 == null){
			        String status;
					status=SaveUserDAO.saveUser(firstname,lastname,emailid,username,password,usertype,monthlyvisitors,companyname,websiteurl,address,state,city,phone,zipcode,token);
			        
					try{
					
				    String url = "<html><body>Hi,<br/><br/><a href='http://localhost:8080/usermod/ConfirmRegistration/"+token+"/'> Click here</a> to confirm Registration</body></html>";
					
				    if(status.equals("true"))
					status1 = SendEmail.sendEmail(emailid,token);
				//    status=UpdateUserDAO.updateUserLoginState("Confirmed", token);
				//	status1 = "true"; 
					}
					 catch(Exception e ){
					 e.printStackTrace();	 
						 
					 }
					
			        
			        if(status1.equals("true")){
			        	
			        	
			        	 response.setType("success");
			        	 response.setDescription("Address registered");
			        	
			        }
			   
			        else{
			        	 
			        	 DeleteEmailDAO.DeleteEmail(emailid);
			        	 response.setType("error");
			        	 response.setDescription("Error in saving User Data");
			        	
			        }
			      
			      }
	           
			        else{
			        	
			        	response.setType("error");
			        	response.setDescription("User Already Exists");
			        
			        
			        }
			        
			        
			        
			        
			        
			        return response;
	           
	           }
			
			
			
			
			

}
