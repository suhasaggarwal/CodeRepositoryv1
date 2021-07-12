package com.personaCache;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
//User Persona DTO
public class UserProfileVersion implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5659765548393227545L;

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

	public Set<String> tags;

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
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

	public Set<String> section;

	public Set<String> getSection() {
		return section;
	}

	public void setSection(Set<String> section) {
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
