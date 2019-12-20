package com.spring.service;



import java.util.List;

import com.spring.model.Reports;



public interface ReportService {
	
	List<Reports> extractReports(long id, String dateRange, String campaignId);
	 
	List<String> extractCampaignIds(long id,String dateRange);


}
