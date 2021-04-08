package com.websystique.springmvc.model;

public class TopicPair {
	public String topic;
	public Integer count;

	public TopicPair(String topic, String count) {
		this.topic = topic;
		this.count = Integer.parseInt(count);
	}
}
