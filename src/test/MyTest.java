package test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class MyTest {
	public static void main(String[] args){
//		Set<String> mainWordSet = new LinkedHashSet<>();
//		String[] text = {"a","a","a","b","c"};
//		for(int i=0; i < text.length; i++){
//			mainWordSet.add(text[i]);
//		}
//		List<String> listString = new ArrayList<>(mainWordSet);
//		java.util.Collections.sort(listString);
//		for(String s: listString){
//			System.out.println(s);
//		}
//		
//		System.out.println("Building Pairs");
//		for(int i=0;i<listString.size()-1; i++){
//			for(int j=i+1; j<listString.size(); j++){
//				String a = listString.get(i);
//				String b = listString.get(j);
//				System.out.println(a + " - " + b);
//			}
//		}
		
//		float num = 0.0005f;
//		String s = String.valueOf(num);
//		System.out.println(s);
//		float n = Float.valueOf(s) + 1.0004f;
//		System.out.println(n);
//		String s2 = "3.0E-4";
//		double d = Double.valueOf(s2) + 1.004d;
//		System.out.println(d);
		
		String test = "000;100|1";
		String[] split = test.split("\\;");
		for(String s: split){
			System.out.println(s);
		}
		System.out.println("Done");
		String test2 = "000;100|1";
		int sepIdx = test2.indexOf('|');
		String pair = test2.substring(0, sepIdx);
		String weight = test2.substring(sepIdx + 1);
		System.out.println(pair + " " + weight);
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
}
