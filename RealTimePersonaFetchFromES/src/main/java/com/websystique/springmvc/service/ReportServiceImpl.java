package com.websystique.springmvc.service;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.websystique.springmvc.model.Reports;

@Service("reportService")
@Transactional

//This API gives cpm,cpc,cpp as well as performance data for all channels cumulative on which a campaign ran.
//Corresponding to this data channel will be depicted as all 

public class ReportServiceImpl implements ReportService{
	
	private static final AtomicLong counter = new AtomicLong();
	 
    public String getUserProfile(HttpServletRequest request){
    
       String json = getCookiePersonaDetails.getCookieData(request);
  
       return json;
      
    
    }


}
