package dataset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class Preprocessor2 {
	
	String outputDir = "experiment/";
	long fileLimit = 1048576L;
	String fileLabel = "100MB";
	String sentenceFile = "_S.txt";
	String paragraphFile = "_P.txt";
	String documentFile = "_D.txt";
	String originalFile = "_ORI.txt";
	String cleanParagraph = "";
	
	public Preprocessor2(String fileLable, long fileLimit){
		this.fileLabel = fileLable;
		this.fileLimit = fileLimit;
	}

	/**
	 * 
	 * @param file
	 * @param nquery
	 * @throws Exception
	 */
	public void execute() {
		
	}
	
	public long writeToFile(String content){
		writeParagraph(content);
//		writeDocument();
		return writeSentence();
	}
	
	private void writeParagraph(String content){
		String filePath = outputDir + fileLabel + paragraphFile;
		try{
			File file =new File(filePath);
			//if file doesnt exists, then create it
			if(!file.exists()){
				file.createNewFile();
			}

			FileWriter fstream = new FileWriter(filePath,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			cleanParagraph = sanitizer(content);
			out.write(cleanParagraph);
			out.close();
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
	}
	
	private long writeSentence(){
		String filePath = outputDir + fileLabel + sentenceFile;
		long size = 0;
		try{
			File file =new File(filePath);
			//if file doesnt exists, then create it
			if(!file.exists()){
				file.createNewFile();
			}

			FileWriter fstream = new FileWriter(filePath,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			List<String> sentences = getSentence(cleanParagraph);
			StringBuilder sb = new StringBuilder();
			for(String s: sentences){			
				sb.append(s + "\n");
			}
			out.write(sb.toString());
			out.close();
			
			size = file.length();
			System.out.println("Done, writing to: " + filePath + " size: " + size);
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
		return size;
	}
	
	private void writeDocument(){
		String filePath = outputDir + fileLabel + documentFile;
		try{
			File file =new File(filePath);
			//if file doesnt exists, then create it
			if(!file.exists()){
				file.createNewFile();
			}

			FileWriter fstream = new FileWriter(filePath,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			String docContent = newLineRemover(cleanParagraph);
			out.write(docContent);
			out.close();
			
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
	}

	private List<String> getSentence(String content) {
		List<String> result = new ArrayList<String>();
		Reader reader = new StringReader(content);
		DocumentPreprocessor dp = new DocumentPreprocessor(reader);
		Iterator<List<HasWord>> it = dp.iterator();
		while (it.hasNext()) {
			List<HasWord> sentence = it.next();
			StringBuilder sentenceSb = new StringBuilder();
			for (HasWord token : sentence) {
				if (sentenceSb.length() > 1) {
					sentenceSb.append(" ");
				}
				sentenceSb.append(token);
			}
			result.add(sentenceSb.toString());
		}
		return result;
	}

	public String printString(List<String> strings) {
		StringBuilder sb = new StringBuilder();
		for (String s : strings) {
			sb.append(s + "\n");
		}
		return sb.toString();
	}
	
	private String newLineRemover(String content){
		String[] temp = content.split("\n");
		StringBuilder sb = new StringBuilder();
		for(String s: temp){
			String c = s.trim();
			if(c.length() > 0){
				sb.append(c);
			}
		}
		return sb.toString();
	}

	private String sanitizer(String content){
		String[] temp = content.split("\n");
		StringBuilder sb = new StringBuilder();
		for(String s: temp){
			String c = s.trim();
			sb.append(c + " ");
			if(c.length() > 0){
				sb.append("\n");
			}
		}
		return sb.toString();
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Preprocessor2 pr = new Preprocessor2("1MB", 1048576L);
		pr.execute();
	}
}
