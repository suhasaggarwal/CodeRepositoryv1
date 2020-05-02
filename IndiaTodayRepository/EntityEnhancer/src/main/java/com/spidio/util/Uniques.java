package com.spidio.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class Uniques {

	public static void main(String[] args) {

		BufferedReader br = null;
		FileReader fr = null;
		String key = null;
		String value = null;
		Set<String> lines1 = new HashSet<String>();

		try {
			List<String> lines = FileUtils.readLines(new File("I://ITWEBENv1.txt"));
			for (String data : lines) {

				lines1.add(data);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Size:" + lines1.size());

	}
}
