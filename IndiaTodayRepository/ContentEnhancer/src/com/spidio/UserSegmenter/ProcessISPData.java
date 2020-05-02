package com.spidio.UserSegmenter;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import com.spidio.dataModel.LocationObject;
import com.spidio.dataModel.LookUpService;
import com.spidio.dataModel.LookUpServiceISP;
import com.spidio.dataModel.LookUpServiceOrganisation;

public class ProcessISPData {

	public static String getISPDetails(String ipAddress) throws Exception {

		// Derive ISP properties of IP Address Using Maxmind
		LookupService cl = LookUpServiceISP.getInstance();

		String isp = cl.getOrg(ipAddress);

		return isp;

	}
}
