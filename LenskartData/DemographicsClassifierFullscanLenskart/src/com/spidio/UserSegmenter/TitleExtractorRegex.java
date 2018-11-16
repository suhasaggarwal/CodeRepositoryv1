package com.spidio.UserSegmenter;

import java.net.MalformedURLException;
import java.net.URL;

public class TitleExtractorRegex {

	
public static String getPageTitle(String url){	
	
URL aURL = null;
try {
	aURL = new URL(url);
} catch (MalformedURLException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}

return aURL.getHost()+aURL.getPath();

}

}