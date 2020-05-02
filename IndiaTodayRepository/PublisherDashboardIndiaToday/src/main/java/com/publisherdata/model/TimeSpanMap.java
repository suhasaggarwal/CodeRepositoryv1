package com.publisherdata.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TimeSpanMap {
	
public String dashboardType; 
public String getDashboardType() {
	return dashboardType;
}
public void setDashboardType(String dashboardType) {
	this.dashboardType = dashboardType;
}
public List<TimeSpanMap> getTimeSpanValues() {
	return timeSpanValues;
}
public void setTimeSpanValues(List<TimeSpanMap> timeSpanValues) {
	this.timeSpanValues = timeSpanValues;
}
public List<TimeSpan> getTimeSpan() {
	return timeSpan;
}
public void setTimeSpan(List<TimeSpan> timeSpan) {
	this.timeSpan = timeSpan;
}
public List<TimeSpanMap> timeSpanValues = new ArrayList<TimeSpanMap>();
public List<TimeSpan> timeSpan = new ArrayList<TimeSpan>();







}
