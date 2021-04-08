package com.websystique.springmvc.model;

public class AffinityPair {
	public String affinitysegment;
    public Integer count;
    
    public AffinityPair(String affinitysegment, String count){
        this.affinitysegment=affinitysegment;
        this.count=Integer.parseInt(count);
    }
}
