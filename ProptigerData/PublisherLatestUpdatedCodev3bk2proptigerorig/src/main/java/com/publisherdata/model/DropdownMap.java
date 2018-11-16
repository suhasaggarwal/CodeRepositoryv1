package com.publisherdata.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DropdownMap {



public String getEndpoint() {
	return endpoint;
}

public void setEndpoint(String endpoint) {
	this.endpoint = endpoint;
}

private String endpoint;

private List<Dropdown> dropdown = new ArrayList<Dropdown>();

public List<Dropdown> getDropdown() {
	return dropdown;
}

public void setDropdown(List<Dropdown> dropdown) {
	this.dropdown = dropdown;
}

private List<DropdownMap> dropdownValues = new ArrayList<DropdownMap>();

public List<DropdownMap> getDropdownValues() {
	return dropdownValues;
}

public void setDropdownValues(List<DropdownMap> dropdownValues) {
	this.dropdownValues = dropdownValues;
}







}
