package com.spidio.dataModel;

public class LocationObject {

	private String city;
	private String country;
	private String postalCode;

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public void setLatittude(float latittude) {
		this.latittude = latittude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	private float latittude;
	private float longitude;

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

	public Float getLatittude() {
		return latittude;
	}

	public void setLatittude(Float latittude) {
		this.latittude = latittude;
	}

	public Float getLongitude() {
		return longitude;
	}

	public void setLongitude(Float longitude) {
		this.longitude = longitude;
	}

}
