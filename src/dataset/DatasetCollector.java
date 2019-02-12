package dataset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class DatasetCollector {
	String sourcePath = "clean/";
	String destinPath = "experiment/";
	
	long fileLimit = 104857600L;
	String fileLabel = "100MB";
	
	String sentenceFile = "S.txt";
	String paragraphFile = "P.txt";
	String documentFile = "D.txt";

	private long senCount = 1;
	private long parCount = 1;
	private long docCount = 1;
	List<String> files = new ArrayList<String>();
	
	List<String> parList = new ArrayList<String>();
	List<String> docList = new ArrayList<String>();
	
	public DatasetCollector(String label, long limit){
		this.fileLabel = label;
		this.fileLimit = limit;
	}
	
	private void getAllBooks(){
		File sourceDir = new File(sourcePath);
		for(File f : sourceDir.listFiles()){
			files.add(f.getName());
		}
	}
	
	public void execute() throws IOException{
		getAllBooks();
		long currentSize = 0;
		for(String f: files){
			List<String> paragraphs = readBookContent(sourcePath + f); 
			currentSize = writeBookContent(paragraphs, destinPath + fileLabel + "/");
			docList.add(docCount +  ":" + (parCount - 1));
			if(currentSize > fileLimit){
				break;
			}
			docCount++;
		}

		String paragraphMapping = destinPath + fileLabel + "/MapPar.txt";
		String documentMapping = destinPath + fileLabel + "/MapDoc.txt";
		
		writeMappingFile(parList, paragraphMapping);
		writeMappingFile(docList, documentMapping);
		System.out.println("DONE~~~");
	}
	
	private void writeMappingFile(List<String> list, String filePath){
		try{
			FileWriter fstream = new FileWriter(filePath,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			for(String s: list){
				out.write(s + "\n");
			}
			out.close();
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
	}
	
	private List<String> readBookContent(String filePath) throws IOException{
		System.out.println("Reading: " + filePath);
		List<String> paragraphs = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(filePath));
	    try {
	        String line = br.readLine();

	        while (line != null) {
	            paragraphs.add(line);
	            line = br.readLine();
	        }
	    } catch (Exception ex){
	    	System.err.println(ex.getMessage());
	    } finally {
	        br.close();
	    }
	    return paragraphs;
	}
	
	private long writeBookContent(List<String> paragraphs, String filePath){
		long size = 0;
		try{
//			writeParagraph(paragraphs, filePath);
			size = writeSentence(paragraphs, filePath);
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
		System.out.println("Done writing: " + filePath);
		return size;
	}
	
	private void writeParagraph(List<String> paragraphs, String filePath){
		String path = filePath + paragraphFile;
		try{
			
			FileWriter fstream = new FileWriter(path,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			for(String paragraph: paragraphs){
				out.write(paragraph + "\n");				
			}
			out.close();
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
	}
	
	private long writeSentence(List<String> paragraphs, String filePath){
		String path = filePath + sentenceFile;
		long size = 0;
		try{
			
			FileWriter fstream = new FileWriter(path,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			for(String paragraph: paragraphs){
				List<String> sentences = getSentence(paragraph);
				StringBuilder sb = new StringBuilder();
				for(String s: sentences){			
					sb.append(senCount + "|" + s + "\n");
					senCount++;
				}
				out.write(sb.toString());
				parList.add(parCount + ":" + (senCount - 1));
				parCount++;
			}			
			out.close();

			File file =new File(path);
			size = file.length();
			System.out.println("Done, writing to: " + path + " size: " + size);
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
		return size;
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
	
	public static void main(String[] args) throws IOException{
		DatasetCollector dc = new DatasetCollector("400MB", 404857000L);
		dc.execute();
	}
}
