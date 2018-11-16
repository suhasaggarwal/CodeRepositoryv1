package com.publisherdata.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Dropdown {

	private String id;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDropdown() {
		return dropdown;
	}
	public void setDropdown(String dropdown) {
		this.dropdown = dropdown;
	}
	private String dropdown;
	
    public String getPageId() {
		return pageId;
	}
	public void setPageId(String pageId) {
		this.pageId = pageId;
	}
	private String pageId;

	private String APIendpoint;
	
	public String getAPIendpoint() {
		return APIendpoint;
	}
	public void setAPIendpoint(String aPIendpoint) {
		APIendpoint = aPIendpoint;
	}
	public String getAPIfilter() {
		return APIfilter;
	}
	public void setAPIfilter(String aPIfilter) {
		APIfilter = aPIfilter;
	}
	private String APIfilter;



}
