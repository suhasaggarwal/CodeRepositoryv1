package com.publisherdata.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.websystique.springmvc.model.Reports;

//Publisher Report Structure Defining Publisher Report Fields
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class PublisherReport {

	   private String urlId;
	
	    public String getUrlId() {
		return urlId;
	}

	public void setUrlId(String urlId) {
		this.urlId = urlId;
	}


		private String stateId;
	    
	    public String getStateId() {
			return stateId;
		}

		public void setStateId(String stateId) {
			this.stateId = stateId;
		}

		public String getCountryId() {
			return countryId;
		}

		public void setCountryId(String countryId) {
			this.countryId = countryId;
		}


		private String countryId;
	
	
	    private String subcategorycode;
	
		public String getSubcategorycode() {
			return subcategorycode;
		}

		public void setSubcategorycode(String subcategorycode) {
			this.subcategorycode = subcategorycode;
		}


		private String date;
		
		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public String getAudience_segment() {
			return audience_segment;
		}

		public void setAudience_segment(String audience_segment) {
			this.audience_segment = audience_segment;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getOs() {
			return os;
		}

		public void setOs(String os) {
			this.os = os;
		}

		
		   public String getISP() {
				return ISP;
			}

			public void setISP(String iSP) {
				ISP = iSP;
			}

			public String getOrganisation() {
				return organisation;
			}

			public void setOrganisation(String organisation) {
				this.organisation = organisation;
			}


			private String ISP;
		    
		    private String organisation;
		
		
		    
		private String subcategory;
	
		
		private String share;
		
		
		public String getExternalWorldShare() {
			return externalWorldShare;
		}

		public void setExternalWorldShare(String externalWorldShare) {
			this.externalWorldShare = externalWorldShare;
		}


		private String externalWorldShare;
		
		private String sharetotalvisits;
		
		public String getSharetotalvisits() {
			return sharetotalvisits;
		}

		public void setSharetotalvisits(String sharetotalvisits) {
			this.sharetotalvisits = sharetotalvisits;
		}

		public String getShareeT() {
			return shareeT;
		}

		public void setShareeT(String shareeT) {
			this.shareeT = shareeT;
		}

		public String getSharevisitorCount() {
			return sharevisitorCount;
		}

		public void setSharevisitorCount(String sharevisitorCount) {
			this.sharevisitorCount = sharevisitorCount;
		}


		private String shareeT;
		
		private String sharevisitorCount;
		
		public String getShare() {
			return share;
		}

		public void setShare(String share) {
			this.share = share;
		}

		public String getAcquisitionDate() {
			return acquisitionDate;
		}

		public void setAcquisitionDate(String acquisitionDate) {
			this.acquisitionDate = acquisitionDate;
		}

		public String getAcquisitionSource() {
			return acquisitionSource;
		}

		public void setAcquisitionSource(String acquisitionSource) {
			this.acquisitionSource = acquisitionSource;
		}

		public String getAcquisitionChannel() {
			return acquisitionChannel;
		}

		public void setAcquisitionChannel(String acquisitionChannel) {
			this.acquisitionChannel = acquisitionChannel;
		}


		private String acquisitionDate;
		
		private String acquisitionSource;
		
		private String acquisitionChannel;
		
		private String deviceImage;
		
		private String sessioncomparison;
		
		
		public String getSessioncomparison() {
			return sessioncomparison;
		}

		public void setSessioncomparison(String sessioncomparison) {
			this.sessioncomparison = sessioncomparison;
		}

		public String getDeviceImage() {
			return deviceImage;
		}

		public void setDeviceImage(String deviceImage) {
			this.deviceImage = deviceImage;
		}

		public String getSubcategory() {
			return subcategory;
		}

		public void setSubcategory(String subcategory) {
			this.subcategory = subcategory;
		}


		 public String getPosts() {
				return posts;
			}

			public void setPosts(String posts) {
				this.posts = posts;
			}


			private String posts;
		
		
		
		private String channelName;
		
		private String browser;
		
		public String getBrowser() {
			return browser;
		}

		public void setBrowser(String browser) {
			this.browser = browser;
		}

		public String getChannelName() {
			return channelName;
		}

		public void setChannelName(String channelName) {
			this.channelName = channelName;
		}

		

		public String getBrandname() {
			return brandname;
		}

		public void setBrandname(String brandname) {
			this.brandname = brandname;
		}


		
		
		
		private List<PublisherReport> audience_segment_data = new ArrayList<PublisherReport>();
		
		public List<PublisherReport> getAudience_segment_data() {
			return audience_segment_data;
		}

		public void setAudience_segment_data(List<PublisherReport> audience_segment_data) {
			this.audience_segment_data = audience_segment_data;
		}

		
		public String getDevice_type() {
			return device_type;
		}

		public void setDevice_type(String device_type) {
			this.device_type = device_type;
		}


		private String authorId;
		
		
		public String getAuthorId() {
			return authorId;
		}

		public void setAuthorId(String authorId) {
			this.authorId = authorId;
		}


		private String device_type;
		
		private String publisher_pages;
		
		private String time_of_day;
		
		private String audience_segment;
		
		private String city;
		
		private String postalcode;
		
		private String latitude_longitude;
		
		private String os;
		
		private String devicecode;
		
		private String audienceSegmentCode;
		
		private String cookie_id;
		
		
		public String getCookie_id() {
			return cookie_id;
		}

		public void setCookie_id(String cookie_id) {
			this.cookie_id = cookie_id;
		}

		public String getAudienceSegmentCode() {
			return audienceSegmentCode;
		}

		public void setAudienceSegmentCode(String audienceSegmentCode) {
			this.audienceSegmentCode = audienceSegmentCode;
		}

		public String getDevicecode() {
			return devicecode;
		}

		public void setDevicecode(String devicecode) {
			this.devicecode = devicecode;
		}

		public String getLocationcode() {
			return locationcode;
		}

		public void setLocationcode(String locationcode) {
			this.locationcode = locationcode;
		}

		public String getOscode() {
			return oscode;
		}

		public void setOscode(String oscode) {
			this.oscode = oscode;
		}


		private String locationcode;
		
		private String oscode;
		
		public String incomelevel;
		
		
		

		public String getIncomelevel() {
			return incomelevel;
		}

		public void setIncomelevel(String incomelevel) {
			this.incomelevel = incomelevel;
		}


		private String mobile_device_model_name;

	    public String getMobile_device_model_name() {
			return mobile_device_model_name;
		}

		public void setMobile_device_model_name(String mobile_device_model_name) {
			this.mobile_device_model_name = mobile_device_model_name;
		}


		private String brandname;
	    
	    public String getArticle() {
			return Article;
		}

		public void setArticle(String article) {
			Article = article;
		}

		public String getSection() {
			return Section;
		}

		public void setSection(String section) {
			Section = section;
		}


		private String Article;
	    
		private String Country;
		
		public String getCountry() {
			return Country;
		}

		public void setCountry(String country) {
			Country = country;
		}

		public String getState() {
			return State;
		}

		public void setState(String state) {
			State = state;
		}


		private String State;
		
	    private String Section;

	    public String getReach() {
			return reach;
		}

		public void setReach(String reach) {
			this.reach = reach;
		}


		private String reach;
	    
		public String getPublisher_pages() {
			return publisher_pages;
		}

		public void setPublisher_pages(String publisher_pages) {
			this.publisher_pages = publisher_pages;
		}

		public String getTime_of_day() {
			return time_of_day;
		}

		public void setTime_of_day(String time_of_day) {
			this.time_of_day = time_of_day;
		}

		public String getPostalcode() {
			return postalcode;
		}

		public void setPostalcode(String postalcode) {
			this.postalcode = postalcode;
		}

		public String getLatitude_longitude() {
			return latitude_longitude;
		}

		public void setLatitude_longitude(String latitude_longitude) {
			this.latitude_longitude = latitude_longitude;
		}

		public String getCount() {
			return count;
		}

		public void setCount(String count) {
			this.count = count;
		}


		private String count;
	    
	    
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

		private String age;
	    
	    private String gender;
	    
	    private String referrerSource;

	    private String screen_properties;
	    
	    
	    public String getScreen_properties() {
			return screen_properties;
		}

		public void setScreen_properties(String screen_properties) {
			this.screen_properties = screen_properties;
		}

		public String getTotalvisits() {
			return totalvisits;
		}

		public void setTotalvisits(String totalvisits) {
			this.totalvisits = totalvisits;
		}


		private String totalvisits;
	    
	    
		public String getReferrerSource() {
			return referrerSource;
		}

		public void setReferrerSource(String referrerSource) {
			this.referrerSource = referrerSource;
		}

		
	
		  public String articleAuthor;

			public String getArticleAuthor() {
				return articleAuthor;
			}

			public void setArticleAuthor(String articleAuthor) {
				this.articleAuthor = articleAuthor;
			}

	 
			public String articleTags;

			public String getArticleTags() {
				return articleTags;
			}

			public void setArticleTags(String articleTags) {
				this.articleTags = articleTags;
			}




          private String externalWorldCount;
          
          public String getExternalWorldCount() {
			return externalWorldCount;
		}

		public void setExternalWorldCount(String externalWorldCount) {
			this.externalWorldCount = externalWorldCount;
		}

		public String getAverageTime() {
			return averageTime;
		}

		public void setAverageTime(String averageTime) {
			this.averageTime = averageTime;
		}


		private String averageTime;


        public String getVisitorCount() {
			return visitorCount;
		}

		public void setVisitorCount(String visitorCount) {
			this.visitorCount = visitorCount;
		}


		private String visitorCount;

        public String getEngagementTime() {
			return engagementTime;
		}

		public void setEngagementTime(String engagementTime) {
			this.engagementTime = engagementTime;
		}


		public String getArticleTitle() {
			return articleTitle;
		}

		public void setArticleTitle(String articleTitle) {
			this.articleTitle = articleTitle;
		}


		private String articleTitle;
		
		private String engagementTime;
        
        private String minutesperVisitor;

		public String getMinutesperVisitor() {
			return minutesperVisitor;
		}

		public void setMinutesperVisitor(String minutesperVisitor) {
			this.minutesperVisitor = minutesperVisitor;
		}
		
     
		private String OSversion;

		public String getOSversion() {
			return OSversion;
		}

		public void setOSversion(String oSversion) {
			OSversion = oSversion;
		}
		
		
        public String getTrafficType() {
			return trafficType;
		}

		public void setTrafficType(String trafficType) {
			this.trafficType = trafficType;
		}

		public String getSiteExperience() {
			return siteExperience;
		}

		public void setSiteExperience(String siteExperience) {
			this.siteExperience = siteExperience;
		}


		private String trafficType;
        
        private String siteExperience;
		
        public String getRetention() {
			return retention;
		}

		public void setRetention(String retention) {
			this.retention = retention;
		}


		private String retention;


		private String ArticleVersion;

		public String getArticleVersion() {
			return ArticleVersion;
		}

		public void setArticleVersion(String articleVersion) {
			ArticleVersion = articleVersion;
		}
		
		
        private String visitorType;

		public String getVisitorType() {
			return visitorType;
		}

		public void setVisitorType(String visitorType) {
			this.visitorType = visitorType;
		}
		
		private String referrerType;

		public String getReferrerType() {
			return referrerType;
		}

		public void setReferrerType(String referrerType) {
			this.referrerType = referrerType;
		}
		
		private String sessionCount;

		public String getSessionCount() {
			return sessionCount;
		}

		public void setSessionCount(String sessionCount) {
			this.sessionCount = sessionCount;
		}

        private String sessionFrequency;
        
        public String getSessionFrequency() {
			return sessionFrequency;
		}

		public void setSessionFrequency(String sessionFrequency) {
			this.sessionFrequency = sessionFrequency;
		}

		
		private String sessionRecency;
		
			
		public String getSessionRecency() {
			return sessionRecency;
		}

		public void setSessionRecency(String sessionRecency) {
			this.sessionRecency = sessionRecency;
		}

		public String getSessionPageDepth() {
			return sessionPageDepth;
		}

		public void setSessionPageDepth(String sessionPageDepth) {
			this.sessionPageDepth = sessionPageDepth;
		}

		public String getSessionDuration() {
			return sessionDuration;
		}

		public void setSessionDuration(String sessionDuration) {
			this.sessionDuration = sessionDuration;
		}


		private String sessionPageDepth;
        private String sessionDuration;

        public String getLikes() {
			return likes;
		}

		public void setLikes(String likes) {
			this.likes = likes;
		}

		public String getShares() {
			return shares;
		}

		public void setShares(String shares) {
			this.shares = shares;
		}


		private String likes;
        private String shares;

        public List<PublisherReport> getChildren() {
			return children;
		}

		public void setChildren(List<PublisherReport> children) {
			this.children = children;
		}


		private String referrerMasterDomain;
		
		
		public String getReferrerMasterDomain() {
			return referrerMasterDomain;
		}

		public void setReferrerMasterDomain(String referrerMasterDomain) {
			this.referrerMasterDomain = referrerMasterDomain;
		}
		
		private String thirdpartycount;


		public String getThirdpartycount() {
			return thirdpartycount;
		}

		public void setThirdpartycount(String thirdpartycount) {
			this.thirdpartycount = thirdpartycount;
		}


		List<PublisherReport> children = new ArrayList<PublisherReport>();
        
        public String getArticleImage() {
			return articleImage;
		}

		public void setArticleImage(String articleImage) {
			this.articleImage = articleImage;
		}


		String articleImage;
        
        
		public String getArticleId() {
			return articleId;
		}

		public void setArticleId(String articleId) {
			this.articleId = articleId;
		}


		private String articleId;
		

       public List<String> getArticleTag() {
			return articleTag;
		}

		public void setArticleTag(List<String> articleTag) {
			this.articleTag = articleTag;
		}


	private List<String> articleTag = new ArrayList<String>();
       
       public String getAudienceSegment() {
		return audienceSegment;
	}

	public void setAudienceSegment(String audienceSegment) {
		this.audienceSegment = audienceSegment;
	}


	private String audienceSegment;
	   
	private String pageViewsPost;

	public String getPageViewsPost() {
		return pageViewsPost;
	}

	public void setPageViewsPost(String pageViewsPost) {
		this.pageViewsPost = pageViewsPost;
	}  
	
	private String scaledShare;

	public String getScaledShare() {
		return scaledShare;
	}

	public void setScaledShare(String scaledShare) {
		this.scaledShare = scaledShare;
	}
	
	public String scaledsharevisits;
	
	
	public String getScaledsharevisits() {
		return scaledsharevisits;
	}

	public void setScaledsharevisits(String scaledsharevisits) {
		this.scaledsharevisits = scaledsharevisits;
	}

	public String getScaledsharevisitors() {
		return scaledsharevisitors;
	}

	public void setScaledsharevisitors(String scaledsharevisitors) {
		this.scaledsharevisitors = scaledsharevisitors;
	}

	public String getScaledshareeT() {
		return scaledshareeT;
	}

	public void setScaledshareeT(String scaledshareeT) {
		this.scaledshareeT = scaledshareeT;
	}


	public String scaledsharevisitors;
	
	
	public String scaledshareeT;

   

	public String tagId;

	public String getTagId() {
		return tagId;
	}

	public void setTagId(String tagId) {
		this.tagId = tagId;
	}

	public String ageId;
	
	public String getAgeId() {
		return ageId;
	}

	public void setAgeId(String ageId) {
		this.ageId = ageId;
	}

	public String getGenderId() {
		return genderId;
	}

	public void setGenderId(String genderId) {
		this.genderId = genderId;
	}

	public String getIncomeId() {
		return incomeId;
	}

	public void setIncomeId(String incomeId) {
		this.incomeId = incomeId;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getReferrerTypeId() {
		return referrerTypeId;
	}

	public void setReferrerTypeId(String referrerTypeId) {
		this.referrerTypeId = referrerTypeId;
	}


	public String getSectionid() {
		return sectionid;
	}

	public void setSectionid(String sectionid) {
		this.sectionid = sectionid;
	}


	public String sectionid;
	
	public String genderId;
	
	public String incomeId;
	
	public String deviceId;
	
	public String referrerTypeId;
	
    public List<PublisherReport> genderlist = new ArrayList<PublisherReport>();
    
    public List<PublisherReport> getGenderlist() {
		return genderlist;
	}

	public void setGenderlist(List<PublisherReport> genderlist) {
		this.genderlist = genderlist;
	}

	public List<PublisherReport> getAgegrouplist() {
		return agegrouplist;
	}

	public void setAgegrouplist(List<PublisherReport> agegrouplist) {
		this.agegrouplist = agegrouplist;
	}

	public List<PublisherReport> getIncomelevellist() {
		return incomelevellist;
	}

	public void setIncomelevellist(List<PublisherReport> incomelevellist) {
		this.incomelevellist = incomelevellist;
	}


	public List<PublisherReport> agegrouplist = new ArrayList<PublisherReport>();
    
    public List<PublisherReport> incomelevellist = new ArrayList<PublisherReport>();
    
    public String scaledExternalWorldShare;

	public String getScaledExternalWorldShare() {
		return scaledExternalWorldShare;
	}

	public void setScaledExternalWorldShare(String scaledExternalWorldShare) {
		this.scaledExternalWorldShare = scaledExternalWorldShare;
	}
    

	
	 
	
	
	public List<PublisherReport> userReport= new ArrayList<PublisherReport>();
	public String activityStats;
	public String duration;


		
		
	public List<PublisherReport> getUserReport() {
		return userReport;
	}

	public void setUserReport(List<PublisherReport> userReport) {
		this.userReport = userReport;
	}

	public String getActivityStats() {
		return activityStats;
	}

	public void setActivityStats(String activityStats) {
		this.activityStats = activityStats;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getActivityType() {
		return activityType;
	}

	public void setActivityType(String activityType) {
		this.activityType = activityType;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}


	public String activityType;
	public String url;

	public String activityImage;

		
	public String getActivityImage() {
		return activityImage;
	}

	public void setActivityImage(String activityImage) {
		this.activityImage = activityImage;
	}


	public String sessiondate;

	public String getSessiondate() {
		return sessiondate;
	}

	public void setSessiondate(String sessiondate) {
		this.sessiondate = sessiondate;
	}	
		
	
	public String scaledsessioncomparison;

	public String getScaledsessioncomparison() {
		return scaledsessioncomparison;
	}

	public void setScaledsessioncomparison(String scaledsessioncomparison) {
		this.scaledsessioncomparison = scaledsessioncomparison;
	}
	

  

	

	
    
    public String scaledsharetotalvisits;

	public String getScaledsharetotalvisits() {
		return scaledsharetotalvisits;
	}

	public void setScaledsharetotalvisits(String scaledsharetotalvisits) {
		this.scaledsharetotalvisits = scaledsharetotalvisits;
	}


   public String getCitylatlong() {
		return citylatlong;
	}

	public void setCitylatlong(String citylatlong) {
		this.citylatlong = citylatlong;
	}


public String citylatlong;


}














