package com.publisherdata.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DashboardTemplate implements Serializable {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8666019172341968070L;
	List<TemplateMap> cardtitletemplate = new ArrayList<TemplateMap>();
	
	public List<TemplateMap> getCardtitletemplate() {
		return cardtitletemplate;
	}
	public void setCardtitletemplate(List<TemplateMap> cardtitletemplate) {
		this.cardtitletemplate = cardtitletemplate;
	}
	public TimeSpanMap getTimespantemplate() {
		return timespantemplate;
	}
	public void setTimespantemplate(TimeSpanMap timespantemplate) {
		this.timespantemplate = timespantemplate;
	}
	public DropdownMap getDropdowntemplate() {
		return dropdowntemplate;
	}
	public void setDropdowntemplate(DropdownMap dropdowntemplate) {
		this.dropdowntemplate = dropdowntemplate;
	}
	TimeSpanMap timespantemplate = new TimeSpanMap();
	DropdownMap dropdowntemplate = new DropdownMap();
	List<Page> pageMap = new ArrayList<Page>();

	public List<Page> getPageMap() {
		return pageMap;
	}
	public void setPageMap(List<Page> pageMap) {
		this.pageMap = pageMap;
	}
	
	
}
