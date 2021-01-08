package com.websystique.springmvc.controller;

import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.publisherdata.model.PublisherReport;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportService;

@RestController
public class ReportRestController {

	@Autowired
	ReportService reportService;  //Service which will do all data retrieval/manipulation work

	//-------------------Retrieve Report with Id--------------------------------------------------------
/*	
	@RequestMapping(value = "/report/{id}/{dateRange}/{channel_name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel_name") String channel_name){
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report = reportService.extractReportsChannelNames(id,dateRange,channel_name);
		if (report == null) {
			System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
	}
*/
	
	@RequestMapping(value = "/report/{id}/{dateRange}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report =  reportService.extractReports(id,dateRange);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}
	
	@RequestMapping(value = "/report/{id}/{dateRange}/{channel}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<PublisherReport>> getReport(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange,@PathVariable("channel") String channel_name) {
		System.out.println("Fetching Report with id " + id);
		List<PublisherReport> report =  reportService.extractReportsChannel(id,dateRange,channel_name);
		if (report == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<List<PublisherReport>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<List<PublisherReport>>(report, HttpStatus.OK);
}
	
	@RequestMapping(value = "/report/{id}/{dateRange}/channelList", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Set<String>> getList(@PathVariable("id") long id,@PathVariable("dateRange") String dateRange) {
		System.out.println("Fetching Report with id " + id);
		Set<String> list =  reportService.extractChannelNames(id,dateRange);
		if (list == null){
		    System.out.println("Report with id " + id + " not found");
			return new ResponseEntity<Set<String>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Set<String>>(list, HttpStatus.OK);
}	

}
