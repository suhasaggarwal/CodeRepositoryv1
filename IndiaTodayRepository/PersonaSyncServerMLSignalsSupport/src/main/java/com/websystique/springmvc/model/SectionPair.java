package com.websystique.springmvc.model;

public class SectionPair {
	public String sectionsegment;
    public Integer count;
    
    public SectionPair(String sectionsegment, String count){
        this.sectionsegment=sectionsegment;
        this.count=Integer.parseInt(count);
    }	
}
