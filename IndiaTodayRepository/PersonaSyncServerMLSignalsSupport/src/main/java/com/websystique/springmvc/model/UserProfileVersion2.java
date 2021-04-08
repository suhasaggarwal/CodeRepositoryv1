package com.websystique.springmvc.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
//User Persona DTO
public class UserProfileVersion2 implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8269481933718530109L;

	/**
	 * 
	 */
	

	public String city;

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
