package com.spidio.UserSegmenter;

import java.io.IOException;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.LocationObject;
import com.spidio.dataModel.LookUpService;

public class ProcessIPAddress {

	public static LocationObject getIPDetails(String ipAddress)
			throws Exception {

		LocationObject obj = new LocationObject();

		// Derive Geo properties of IP Address using MaxMind

		LookupService cl = LookUpService.getInstance();
		Location l1 = cl.getLocation(ipAddress);
		if (l1 != null) {
			System.out.println("countryCode: "
					+ l1.countryCode
					+ "\n countryName: "
					+ l1.countryName
					+ "\n region: "
					+ l1.region
					+ "\n regionName: "
					+ regionName.regionNameByCode(l1.countryCode, l1.region)
					+ "\n city: "
					+ l1.city
					+ "\n postalCode: "
					+ l1.postalCode
					+ "\n latitude: "
					+ l1.latitude
					+ "\n longitude: "
					+ l1.longitude
					+ "\n distance: "
					+ l1.distance(l1)
					+ "\n metro code: "
					+ l1.metro_code
					+ "\n area code: "
					+ l1.area_code
					+ "\n timezone: "
					+ timeZone.timeZoneByCountryAndRegion(l1.countryCode,
							l1.region));

			obj.setCity(l1.city);
		
			obj.setCountry(l1.countryName);
			obj.setLatittude(l1.latitude);
			obj.setLongitude(l1.longitude);
			obj.setPostalCode(l1.postalCode);
		    obj.setState(regionName.regionNameByCode(l1.countryCode, l1.region));
		   
		}
		cl.close();

		return obj;

	}

}
