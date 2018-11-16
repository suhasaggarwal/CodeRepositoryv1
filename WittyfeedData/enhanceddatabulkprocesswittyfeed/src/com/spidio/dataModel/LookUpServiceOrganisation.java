package com.spidio.dataModel;

import java.io.IOException;

import com.maxmind.geoip.LookupService;

public class LookUpServiceOrganisation {

	public static LookupService lookup;

	private static LookupService INSTANCE = null;

	public static synchronized LookupService getInstance() {

		if (INSTANCE == null) {
			try {
				INSTANCE = new LookupService("/root/GeoIPOrg.dat",
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
