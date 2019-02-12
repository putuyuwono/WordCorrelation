package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class StopwordRemover {
	private Set<String> stopWords;
    public StopwordRemover() {
        stopWords = new LinkedHashSet<>();
    }

    public void loadStopWordFromFile(String inputFile) {        
        try{
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            for (String line; (line = br.readLine()) != null;) {
                stopWords.add(line.trim());
            }
            br.close();
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }           
    }
    
    public String removeStopword(String inputText){
        StringBuilder sb = new StringBuilder();
        String[] words = inputText.replaceAll("\\W", " ").split(" ");
        for (String word : Arrays.asList(words)) {
            if(!stopWords.contains(word.toLowerCase())){
                sb.append(word + " ");
            }
        }
        return sb.toString();
    }
    
    public static void main (String[] args){
    	String[] stopwordFiles = {"data/stopword/1.txt", "data/stopword/2.txt", "data/stopword/3.txt", "data/stopword/4.txt", "data/stopword/5.txt"};
    	StopwordRemover swRemover = new StopwordRemover();
        for (String inputFile : stopwordFiles) {
            swRemover.loadStopWordFromFile(inputFile);
        }
        
        for(String s: swRemover.stopWords){
        	System.out.print("\""+s + "\", ");
        }
    }
}
