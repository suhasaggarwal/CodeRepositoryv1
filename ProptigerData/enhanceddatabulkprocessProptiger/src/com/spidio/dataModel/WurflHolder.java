package com.spidio.dataModel;

import java.io.IOException;

import net.sourceforge.wurfl.core.CustomWURFLHolder;
import net.sourceforge.wurfl.core.WURFLHolder;

public class WurflHolder {

	public static WURFLHolder wurflHolder;

	private static WURFLHolder INSTANCE = null;

	public static synchronized WURFLHolder getInstance() {

		if (INSTANCE == null) {
			INSTANCE = new CustomWURFLHolder("/root/wurfl.xml");
		}

		else {
			return INSTANCE;
		}

		return INSTANCE;

	}

}
