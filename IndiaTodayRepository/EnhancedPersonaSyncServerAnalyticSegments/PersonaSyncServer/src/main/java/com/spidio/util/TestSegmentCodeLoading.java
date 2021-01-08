package com.spidio.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.gson.Gson;
import com.personaCache.EnhanceUserDataDaily;
import com.websystique.springmvc.model.UserProfile;

public class TestSegmentCodeLoading {

	
	public static void main(String[] args) {

		Set<String> result = new HashSet<String>();
		Set<String> latestFrequentCategories = new LinkedHashSet<String>();
		Set<String> latestFrequentCategories1 = new LinkedHashSet<String>();
		ArrayList<String> mostFrequentCategories = new ArrayList<String>();
		ArrayList<String> mostFrequentCategories1 = new ArrayList<String>();
		ArrayList<String> taglist = new ArrayList<String>();
		Set<String> latestFrequentSection = new LinkedHashSet<String>();
		Set<String> latestFrequentSection1 = new LinkedHashSet<String>();
		ArrayList<String> mostFrequentSection = new ArrayList<String>();
		ArrayList<String> mostFrequentSection1 = new ArrayList<String>(); 		
		
		
		latestFrequentCategories.add("_home.and.garden_home.improvement.and.repair_");
		latestFrequentCategories.add("_home.and.garden_home.improvement.and.repair_roofing_");
		latestFrequentCategories.add("_home.and.garden_home.furnishings_rugs.and.carpets_");
		latestFrequentCategories.add("_home.and.garden_bed.and.bath_bedroom_bedding.and.bed.linens_");
	   
		
		mostFrequentCategories.add("_home.and.garden_home.improvement.and.repair_");
		mostFrequentCategories.add("_home.and.garden_home.improvement.and.repair_roofing_");
		mostFrequentCategories.add("_home.and.garden_home.furnishings_rugs.and.carpets_");
		mostFrequentCategories.add("_home.and.garden_bed.and.bath_bedroom_bedding.and.bed.linens_");
	

		
	for (String s1 : latestFrequentCategories) {
		System.out.println(EnhanceUserDataDaily.audienceSegmentMap2.get(s1));
		latestFrequentCategories1.add(EnhanceUserDataDaily.audienceSegmentMap2.get(s1));
		//latestFrequentCategories1.add(s1.toLowerCase().replace("_", " ").replaceAll("\\.", " "));
	}

	System.out.println();
	
	for (String s2 : mostFrequentCategories) {
		System.out.println(EnhanceUserDataDaily.audienceSegmentMap2.get(s2));
		mostFrequentCategories1.add(EnhanceUserDataDaily.audienceSegmentMap2.get(s2));
		//mostFrequentCategories1.add(s2.toLowerCase().replace("_", " ").replaceAll("\\.", " "));
	}

	UserProfile profile = new UserProfile();
	profile.setCity("Delhi");
	profile.setCountry("India");
    profile.setMobileDevice("Samsung Galaxy S10");
    profile.setInMarketSegments(latestFrequentCategories1);
    profile.setAffinitySegments(mostFrequentCategories1);
    profile.setSection(latestFrequentSection);
    profile.setGender("male");
    profile.setAge("1");
    profile.setIncomelevel("high");
    profile.setTags(result);

	String json = new Gson().toJson(profile);
	System.out.println("Persona1:" + json);

	StringBuilder sbf = new StringBuilder("");
    sbf.append("Delhi".toLowerCase());
	sbf.append("@@");
	sbf.append("India".toLowerCase());
	sbf.append("@@");
	sbf.append("Samsung Galaxy S10".toLowerCase());
	sbf.append("@@");
    sbf.append(String.join(",", latestFrequentCategories1));
	sbf.append("@@");
	sbf.append(String.join(",", mostFrequentCategories1));
    sbf.append("@@");
	sbf.append(String.join(",", latestFrequentSection));
	sbf.append("@@");
	sbf.append("male");
	sbf.append("@@");
	sbf.append("1");
	sbf.append("@@");
	sbf.append("high");
	sbf.append("@@");
	sbf.append(String.join(",", result));
	System.out.println("Persona:" + sbf.toString());
	
	
	
	
	}


}
