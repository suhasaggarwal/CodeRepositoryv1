package com.spidio.UserSegmenter;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import com.spidio.dataModel.LocationObject;
import com.spidio.dataModel.LookUpService;
import com.spidio.dataModel.LookUpServiceOrganisation;

public class ProcessOrganisationData {

	// Derive Organization Details of IP Address using maxmind
	// If organization name cannot be derived, maxmind will give corresponding
	// ISP name

	public static String getOrgDetails(String ipAddress) throws Exception {

		LookupService cl = LookUpServiceOrganisation.getInstance();

		String org = cl.getOrg(ipAddress);

		return org;
	}

}
