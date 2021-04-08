package com.websystique.springmvc.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
//User Persona DTO
public class UserProfile implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7031775377550764673L;

	public String city;

	public String mostactiveQuarter;
	
	public String getMostactiveQuarter() {
		return mostactiveQuarter;
	}

	public void setMostactiveQuarter(String mostactiveQuarter) {
		this.mostactiveQuarter = mostactiveQuarter;
	}

	public Integer totalnumberofsessions;
	
	public Integer getTotalnumberofsessions() {
		return totalnumberofsessions;
	}

	public void setTotalnumberofsessions(Integer totalnumberofsessions) {
		this.totalnumberofsessions = totalnumberofsessions;
	}

	public Integer getTotalengagementtime() {
		return totalengagementtime;
	}

	public void setTotalengagementtime(Integer totalengagementtime) {
		this.totalengagementtime = totalengagementtime;
	}

	public Integer hoursincelastvisit;
	
	public Integer getHoursincelastvisit() {
		return hoursincelastvisit;
	}

	public void setHoursincelastvisit(Integer hoursincelastvisit) {
		this.hoursincelastvisit = hoursincelastvisit;
	}

	public Integer totalengagementtime; 
	
	
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getMobileDevice() {
		return mobileDevice;
	}

	public void setMobileDevice(String mobileDevice) {
		this.mobileDevice = mobileDevice;
	}

	public List<String> getAffinitySegments() {
		return AffinitySegments;
	}

	public void setAffinitySegments(List<String> affinitySegments) {
		this.AffinitySegments = affinitySegments;
	}

	public String country;

	public String mobileDevice;

	public List<String> tags;

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public Set<String> inMarketSegments;

	public Set<String> getInMarketSegments() {
		return inMarketSegments;
	}

	public void setInMarketSegments(Set<String> inMarketSegments) {
		this.inMarketSegments = inMarketSegments;
	}

	public List<String> AffinitySegments;

	public List<String> SectionEngagementTimes;
	
	public List<String> getSectionEngagementTimes() {
		return SectionEngagementTimes;
	}

	public void setSectionEngagementTimes(List<String> sectionEngagementTimes) {
		SectionEngagementTimes = sectionEngagementTimes;
	}

	public List<String> getSegmentEngagementTimes() {
		return SegmentEngagementTimes;
	}

	public void setSegmentEngagementTimes(List<String> segmentEngagementTimes) {
		SegmentEngagementTimes = segmentEngagementTimes;
	}

	public List<String> getTopicEngagementTimes() {
		return TopicEngagementTimes;
	}

	public void setTopicEngagementTimes(List<String> topicEngagementTimes) {
		TopicEngagementTimes = topicEngagementTimes;
	}

	public List<String> SegmentEngagementTimes;
	
	public List<String> TopicEngagementTimes;
	
	public List<String> section;

	public List<String> getSection() {
		return section;
	}

	public void setSection(List<String> section) {
		this.section = section;
	}

	public String age;

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getIncomelevel() {
		return incomelevel;
	}

	public void setIncomelevel(String incomelevel) {
		this.incomelevel = incomelevel;
	}

	public String gender;

	public String incomelevel;

}
