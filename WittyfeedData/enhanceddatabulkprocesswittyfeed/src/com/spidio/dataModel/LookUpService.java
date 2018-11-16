package com.spidio.dataModel;

import java.io.IOException;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import com.maxmind.geoip.LookupService;

public class LookUpService {

	public static LookupService lookup;

	private static LookupService INSTANCE = null;

	public static synchronized LookupService getInstance() {

		if (INSTANCE == null) {
			try {
				INSTANCE = new LookupService("/root/GeoLiteCity.dat",
						LookupService.GEOIP_MEMORY_CACHE);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			return INSTANCE;
		}

		return INSTANCE;

	}

}
