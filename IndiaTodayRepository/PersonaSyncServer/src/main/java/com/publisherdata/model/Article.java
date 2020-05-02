package com.publisherdata.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Article implements Serializable {
	   
	
		

	/**
	 * 
	 */
	private static final long serialVersionUID = 8501438273073017649L;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getArticleUrl() {
		return articleUrl;
	}
	public void setArticleUrl(String articleUrl) {
		this.articleUrl = articleUrl;
	}
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	public String getSectionid() {
		return sectionid;
	}
	public void setSectionid(String sectionid) {
		this.sectionid = sectionid;
	}
	private String id;
	private String articleUrl;
    private String siteId;
    private String sectionid;
    private String Author;
    public String getAuthor() {
		return Author;
	}
	public void setAuthor(String author) {
		Author = author;
	}
	public String getPublishdate() {
		return publishdate;
	}
	public void setPublishdate(String publishdate) {
		this.publishdate = publishdate;
	}
	public String getMainimage() {
		return mainimage;
	}
	public void setMainimage(String mainimage) {
		this.mainimage = mainimage;
	}
	
	private String publishdate;
    private String mainimage;
    
    private List<String> tags = new ArrayList<String>();
    
    public List<String> getTags() {
		return tags;
	}
	
    public void setTags(List<String> tags) {
		this.tags = tags;
	}
	public String getArticletitle() {
		return articletitle;
	}
	public void setArticletitle(String articletitle) {
		this.articletitle = articletitle;
	}
	private String articletitle;
    public String getArticleName() {
		return articleName;
	}
	public void setArticleName(String articleName) {
		this.articleName = articleName;
	}
	private String articleName;

    private String Sectionname;
	
	
    public String getSectionname() {
		return Sectionname;
	}
	public void setSectionname(String sectionname) {
		Sectionname = sectionname;
	}
	private String authorId;
    
	public String getAuthorId() {
		return authorId;
	}
	public void setAuthorId(String authorId) {
		this.authorId = authorId;
	}

	 public List<String> getTagIds() {
		return tagIds;
	}
	public void setTagIds(List<String> tagIds) {
		this.tagIds = tagIds;
	}
	private List<String> tagIds = new ArrayList<String>();
	 
	 
}
