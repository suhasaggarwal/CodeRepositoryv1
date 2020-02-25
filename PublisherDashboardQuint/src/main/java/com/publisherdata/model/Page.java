package com.publisherdata.model;

import com.fasterxml.jackson.annotation.JsonInclude;


@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Page {

private String PageId;
public String getPageId() {
	return PageId;
}
public void setPageId(String pageId) {
	PageId = pageId;
}
public String getPageName() {
	return pageName;
}
public void setPageName(String pageName) {
	this.pageName = pageName;
}
public String getPageTitle() {
	return pageTitle;
}
public void setPageTitle(String pageTitle) {
	this.pageTitle = pageTitle;
}
public String getPageSubtitle() {
	return pageSubtitle;
}
public void setPageSubtitle(String pageSubtitle) {
	this.pageSubtitle = pageSubtitle;
}
public String getParentPageId() {
	return parentPageId;
}
public void setParentPageId(String parentPageId) {
	this.parentPageId = parentPageId;
}
private String pageName;
private String pageTitle;
private String pageSubtitle;
private String parentPageId;





}
