package com.publisherdata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TimeSpan {

private String id;
public String getId() {
	return id;
}
public void setId(String id) {
	this.id = id;
}
public String getTimespan() {
	return timespan;
}
public void setTimespan(String timespan) {
	this.timespan = timespan;
}
private String timespan;

private String minutes;
public String getMinutes() {
	return minutes;
}
public void setMinutes(String minutes) {
	this.minutes = minutes;
}



}
