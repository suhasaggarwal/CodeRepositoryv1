package com.personaCache;
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

	public String admiration;
	public String amusement;
	public String anger;
	public String annoyance;
	public String approval;
	public String caring;
	public String confusion;
	public String curiosity;
	public String desire;
	public String disappointment;
	public String disapproval;
	public String disgust;
	public String embarrassment;
	public String excitement;
	public String fear;
	public String gratitude;
	public String grief;
	public String joy;
	public String love;
	public String nervousness;
	public String optimism;
	public String pride;
	public String realization;
	public String relief;
	public String remorse;
	public String sadness;
	public String surprise;
	public String neutral;

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

	public List<String> inMarketSegments;

	public List<String> getInMarketSegments() {
		return inMarketSegments;
	}

	public void setInMarketSegments(List<String> inMarketSegments) {
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

	public List<String> age;

	public List<String> getAge() {
		return age;
	}

	public void setAge(List<String> age) {
		this.age = age;
	}

	public List<String> getGender() {
		return gender;
	}

	public void setGender(List<String> gender) {
		this.gender = gender;
	}

	public List<String> getIncomelevel() {
		return incomelevel;
	}

	public void setIncomelevel(List<String> incomelevel) {
		this.incomelevel = incomelevel;
	}

	public List<String> gender;

	public List<String> incomelevel;

}
