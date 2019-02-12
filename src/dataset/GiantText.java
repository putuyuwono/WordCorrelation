package dataset;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class GiantText {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	private String user = "root";
	private String pass = "root";
	private String url = "jdbc:mysql://node05:3306/biography";
	private static String ROOTDIR = "giantart2/";
	private static String LEVBASED = "/LevelBased/";
	private static String WORDCOUNT = "/WordCount/";
	private static int MAX_FILES = 8000;
	private Map<String, String> bios = new HashMap<String, String>();

	public void printToSentence(Map.Entry<String, String> entry) {
		String name = entry.getKey();
		String content = entry.getValue();
		List<String> sentences = getSentence(content);
		String outputDir = ROOTDIR + LEVBASED;
		String output = outputDir + name  + "S.txt";
		try {
			Files.createDirectories(Paths.get(outputDir));
			FileWriter fstream = new FileWriter(output);
			BufferedWriter out = new BufferedWriter(fstream);
			for(String s: sentences){
				out.write(s + "\n");
			}
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void printToParagraph(Map.Entry<String, String> entry) {
		String name = entry.getKey();
		String content = entry.getValue();
		String outputDir = ROOTDIR + LEVBASED;
		String output = outputDir + name  + "P.txt";
		try {
			Files.createDirectories(Paths.get(outputDir));
			FileWriter fstream = new FileWriter(output);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(content);
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void printToDocument(Map.Entry<String, String> entry) {
		String name = entry.getKey();
		String content = entry.getValue();
		List<String> sentences = getSentence(content);
		String outputDir1 = ROOTDIR + LEVBASED;
		String outputDir2 = ROOTDIR + WORDCOUNT;
		String output1 = outputDir1 + name  + "D.txt";
		String output2 = outputDir2 + name  + "D.txt";
		try {
			Files.createDirectories(Paths.get(outputDir1));
			FileWriter fstream1 = new FileWriter(output1);
			BufferedWriter out1 = new BufferedWriter(fstream1);
			for(String s: sentences){
				out1.write(s + " ");
			}
			out1.close();
			
			Files.createDirectories(Paths.get(outputDir2));
			FileWriter fstream2 = new FileWriter(output2);
			BufferedWriter out2 = new BufferedWriter(fstream2);
			for(String s: sentences){
				out2.write(s + " ");
			}
			out2.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
	
	/**
	 * Splitting String into Sentences
	 * @param content
	 * @return List of sentences
	 */
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

	private void createDocument(Map.Entry<String, String> entry){
		printToSentence(entry);
		printToParagraph(entry);
		printToDocument(entry);
	}
	
	public void preprocessDocument(){
		System.out.println("Printing " + bios.size() + " documents");
		for(Map.Entry<String, String> entry: bios.entrySet()){
			createDocument(entry);
		}
		System.out.println("Done");
	}
	
	public void collect() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(url, user, pass);
			statement = connect.createStatement();
			String query = "SELECT bio_content FROM biography";
			resultSet = statement.executeQuery(query);
			StringBuilder sb = new StringBuilder();
			int counter = 1;
			int splitter = 7000;
			String content;
			while (resultSet.next() && counter < 2) {
				content = resultSet.getString("bio_content").replaceAll("<br>", "");
				if (!content.isEmpty()) {
					sb.append(content + "\n");
					counter++;
				}
				
				if((counter%splitter) == 0){
					bios.put(String.valueOf(counter), sb.toString());
					sb = new StringBuilder();
					System.out.println("It's been written");
				}
				
			}
			bios.put(String.valueOf(counter), sb.toString());
			connect.close();
		} catch (Exception e) {
		}
	}
	
	public void collect2() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(url, user, pass);
			statement = connect.createStatement();
			String query = "SELECT * FROM biography";
			resultSet = statement.executeQuery(query);
			int counter = 1;
			while (resultSet.next()) {
				String name = String.valueOf(counter);
				String content = resultSet.getString("bio_content").replaceAll("<br>", "");
				if (!content.isEmpty()) {
					bios.put(name, content);
					counter++;
				}

				if (counter >= MAX_FILES) {
					break;
				}
			}
			connect.close();
		} catch (Exception e) {
		}
	}

	public static void main(String[] args) {
		GiantText pre = new GiantText();
		pre.collect2();
		pre.preprocessDocument();
	}
}
