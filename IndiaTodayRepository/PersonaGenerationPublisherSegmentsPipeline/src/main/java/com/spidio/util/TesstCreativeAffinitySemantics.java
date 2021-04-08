package com.spidio.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TesstCreativeAffinitySemantics {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int array1[] = {39,33,48,39,45,67,56,52,45,53,65,65,51,46,52,
				49,48,44,53,60,67,67,56,40,45,55,62,45,31,39,32,53,52,40,36,40,66,47,45,34,72,51,63,57,46,62,38,60,70,59,62,60,54,54,51,49,38,36,52,35,55,44,37,46,56,73,60,57,51,51,51,38,48,44,44,46,47,51,49,41,48,55,50,52,53,48,48,62,48,51,53,38,56,59,43,54,42,60,40,37,53,38,50,47,57,53,42,57,35,76,69,53,51,67,61,62,61,61,43,56,56,69,62,46,66,47,62,47,45,50,57,56,71,41,47,47,66,37,55,73,41,48,40,39,37,56,36,57,60,57,56,32,35,61,57,37,54,36,33,59,56,53,50,41,47,47,62,47,46,61,60,54,51,
				53,56,42,57,50,37,38,54,38,50,50,50,66,56,56,51,44,59,43,59,38,53,53,48,56,45,66};
		System.out.println(array1.length);
		List<Element> elements = new ArrayList<Element>();
		for (int i = 0; i < array1.length; i++) {
		    elements.add(new Element(i, array1[i]));
		}
		Map<Integer, Integer> Idmap = new HashMap<Integer,Integer>();
		BufferedReader br = null;
		String line= null;	
		Integer i = 0;
		
			try {
				br = new BufferedReader(new FileReader("I://logb1.txt"));
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}

			try {
				while ((line = br.readLine()) != null) {

					try {
						// System.out.println(line);
						//System.out.println(line.split(":")[1].replace("wiki",""));
						Idmap.put(i+1, new Integer(line.split(":")[1].replace("wiki","")));
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}
                       i++;
				}
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}

			Map<Integer,String> CreativeIdmap = new HashMap<Integer,String>();
			BufferedReader br1 = null;
			String line1= null;	
			Integer i1= 0;
			
				try {
					br1 = new BufferedReader(new FileReader("I://CreativeDatabasev1.txt"));
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				try {
					while ((line1 = br1.readLine()) != null) {

						try {
							// System.out.println(line);
							//System.out.println(line1);
							CreativeIdmap.put(i1+1, line1);
						} catch (Exception e) {
							e.printStackTrace();
							continue;
						}
                        i1++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		for(Entry<Integer,Integer> a2 : Idmap.entrySet()) {
		   //System.out.println(a2.getKey() + ':' + a2.getValue());
		}
		
        for(Entry<Integer,String> a3 : CreativeIdmap.entrySet()) {
        	//System.out.println(a3.getKey() + ':' + a3.getValue());
		}
		// Sort and print
		
        Collections.sort(elements);
		Collections.reverse(elements); // If you want reverse order
		for (Element element : elements)  {
			Integer value = element.index+1;
			Integer data = Idmap.get(value);
			System.out.println(element.value + ":" + CreativeIdmap.get(data));
		}
		
		
	}

}
