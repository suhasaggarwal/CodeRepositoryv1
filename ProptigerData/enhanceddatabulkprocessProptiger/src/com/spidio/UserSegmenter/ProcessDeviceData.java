package com.spidio.UserSegmenter;

import net.sourceforge.wurfl.core.CustomWURFLHolder;
import net.sourceforge.wurfl.core.Device;
import net.sourceforge.wurfl.core.WURFLHolder;
import net.sourceforge.wurfl.core.WURFLManager;

import com.spidio.dataModel.DeviceObject;
import com.spidio.dataModel.WurflHolder;

public class ProcessDeviceData {

	public static DeviceObject getDeviceDetails(String userAgent)
			throws Exception {

		// Derive Properties Corresponding to the device using Wurfl
		// Phone Models of year 2016 release date are also detected

		DeviceObject obj = new DeviceObject();

		String brandName, model_name, marketing_name, deviceOs, device_os_version, release_date, physical_screen_width, physical_screen_height, wireless_device, tablet,browser, resolution_width, resolution_height;
		// WURFLHolder wurflHolder = new
		// CustomWURFLHolder(ProcessDeviceData.class.getResource("/wurfl.xml").toString());
		// WURFLHolder wurflHolder = new CustomWURFLHolder("C://wurfl.xml");
		WURFLHolder wurflHolder = WurflHolder.getInstance();

		WURFLManager wurflManager = wurflHolder.getWURFLManager();
		// String
		// ua="Mozilla/5.0 (Linux; U; Android 2.1-update1; en-us; Nexus One Build/ERE27) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17";
		// String ua =
		// "Mozilla/5.0 (iPad; CPU OS 9_2_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13D15";
		// String ua =
		// "Mozilla/5.0 (Linux; Android 5.1; Iris X8 L Build/LMY47I) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/39.0.0.0 Mobile Safari/537.36";
		// String ua =
		// "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36";

		// String ua =
		// "Mozilla/5.0 (Linux; Android 5.0; Cloud 4G Star Build/LRX21M) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile Safari/537.36";

		// String ua =
		// "Mozilla/5.0 (Linux; Android 5.0.2; XT1225 Build/LXG22.33-12.16; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/50.0.2661.86 Mobile Safari/537.36";

		// String ua =
		// "Mozilla/5.0 (Linux; U; Android 4.1.2; en-gb; GT-P3100 Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30";

		// String ua =
		// "Mozilla/5.0 (Linux; U; Android 4.2.2; en-US; GT-S7270 Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.5.0.575 U3/0.8.0 Mobile Safari/534.30";

		// String ua = "Peeplo Screenshot Bot/0.20 ( abuse at peeplo dot_com )";

		// String ua =
		// "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html";

		
		String picture_bmp,picture_gif,picture_jpg,picture_png,gif_animated,streaming_video,streaming_3gpp,streaming_mp4,streaming_mov,colors,dual_orientation,ux_full_desktop;
		
		
		Device device = wurflManager.getDeviceForRequest(userAgent);
		String browserversion = device.getCapability("mobile_browser_version");
		picture_bmp = device.getCapability("picture_bmp");
	    picture_gif = device.getCapability("picture_gif");
	    picture_jpg = device.getCapability("picture_jpg");
	    picture_png = device.getCapability("picture_png");
	    gif_animated = device.getCapability("gif_animated");
	    streaming_video = device.getCapability("streaming_video");
	    streaming_3gpp = device.getCapability("streaming_3gpp");
	    streaming_mp4 = device.getCapability("streaming_mp4");
	    streaming_mov =  device.getCapability("streaming_mov");
	    colors = device.getCapability("colors");
	    dual_orientation = device.getCapability("dual_orientation");
	    ux_full_desktop = device.getCapability("ux_full_desktop");
		obj.setPicture_bmp(picture_bmp);
	    obj.setPicture_gif(picture_gif);
		obj.setPicture_jpg(picture_jpg);
		obj.setPicture_png(picture_png);
		obj.setGif_animated(gif_animated);
		obj.setStreaming_video(streaming_video);
		obj.setStreaming_3gpp(streaming_3gpp);
		obj.setStreaming_mp4(streaming_mp4);
		obj.setStreaming_mov(streaming_mov);
		obj.setColors(colors);
		obj.setDual_orientation(dual_orientation);
		obj.setUx_full_desktop(ux_full_desktop);
		
		browser = device.getCapability("mobile_browser");
		obj.setBrowser(browser);
		brandName = device.getCapability("brand_name");
		obj.setBrandName(brandName);
		model_name = device.getCapability("model_name");
		obj.setModel_name(model_name);
		marketing_name = device.getCapability("marketing_name");
		obj.setMarketing_name(marketing_name);
		deviceOs = device.getCapability("device_os");
		obj.setDeviceOs(deviceOs);
		device_os_version = device.getCapability("device_os_version");
		obj.setDevice_os_version(device_os_version);
		release_date = device.getCapability("release_date");
		obj.setRelease_date(release_date);
		physical_screen_width = device.getCapability("physical_screen_width");
		obj.setPhysical_screen_width(physical_screen_width);
		physical_screen_height = device.getCapability("physical_screen_height");
		obj.setPhysical_screen_height(physical_screen_height);
		wireless_device = device.getCapability("is_wireless_device");
		obj.setWireless_device(wireless_device);
		tablet = device.getCapability("is_tablet");
		obj.setIs_tablet(tablet);
		resolution_width = device.getCapability("resolution_width");
		obj.setResolution_width(resolution_width);
		resolution_height = device.getCapability("resolution_height");
		obj.setResolution_height(resolution_height);
		obj.setBrowserversion(browserversion);
		System.out.println("BrandName:" + brandName);
		System.out.println("OS:" + deviceOs);
		System.out.println("Model:" + model_name);
		System.out.println("MarketingName:" + marketing_name);
		System.out.println("Version:" + device_os_version);
		System.out.println("Date:" + release_date);
		System.out.println("ScreenWidth:" + physical_screen_width);
		System.out.println("ScreenHeight:" + physical_screen_height);
		System.out.println("Wireless_Device:" + wireless_device);
		System.out.println("Resolution_Width:" + resolution_width);
		System.out.println("Resoultion_height:" + resolution_height);

		// System.out.println(device.getCapabilities());

		return obj;

	}

}