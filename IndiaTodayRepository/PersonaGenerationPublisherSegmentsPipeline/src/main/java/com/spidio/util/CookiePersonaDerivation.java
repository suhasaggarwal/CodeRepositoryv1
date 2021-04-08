package com.spidio.util;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CookiePersonaDerivation {

	public static StringBuffer inputBuffer = new StringBuffer();

	public static void main(String[] args) throws Exception {
		try {

			latestCookiePersonaExtractor(args[0], args[1]);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// read file one line at a time
	// replace line as you read the file and store updated lines in StringBuffer
	// overwrite the file with the new lines
	public static void latestCookiePersonaExtractor(String filename, String outputfilename) {

		Set<String> cookieSet = new LinkedHashSet<String>();
		
		StringBuffer inputBuffer = new StringBuffer();
		String txtFile = "";
		BufferedReader br = null;
		FileWriter fw = null;
		String line = "";
		String[] parts;
		String cvsSplitBy5 = ",";
		String key2 = "";
		String key = "";
		String value = "";
		String regex = "\\s+";
		try {
			br = new BufferedReader(new FileReader(filename));

			while ((line = br.readLine()) != null) {

				try {
					// System.out.println(line);
					cookieSet.add(line.trim().toLowerCase());
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}

			}

			Integer i = 0;
			for (String entry : cookieSet) {
				i++;
				key = entry.toLowerCase();
				line = i + ":@" + key;
				line = line.toLowerCase().replaceAll("and","").replaceAll("'s","").replaceAll("!","").replaceAll(regex, ",");
				inputBuffer.append(line);
				inputBuffer.append('\n');
				if (inputBuffer.length() >= 100000) {
					WriteBatch(inputBuffer.toString(),outputfilename);
					inputBuffer.delete(0, inputBuffer.length());
				}
			}
			WriteBatch(inputBuffer.toString(),outputfilename);
			
		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	public static void WriteBatch(String Buffer, String outputfilename) {

		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(outputfilename, false));
		    out.write(Buffer);
		    out.close();
		}
		 catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	   } 

	}
}
