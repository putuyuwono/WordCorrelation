package dataset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BookPreprocessor {
	String sourcePath = "books/";
	String destinPath = "clean/";
	List<String> files = new ArrayList<String>();
	
	private void getAllBooks(){
		File sourceDir = new File(sourcePath);
		for(File f : sourceDir.listFiles()){
			files.add(f.getName());
		}
	}
	
	public void preprocessBooks() throws IOException{
		getAllBooks();
		for(String f: files){
			List<String> paragraphs = readBookContent(sourcePath + f); 
			writeBookContent(paragraphs, destinPath + f);
		}
	}
	
	private List<String> readBookContent(String filePath) throws IOException{
		System.out.println("Reading: " + filePath);
		List<String> paragraphs = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(filePath));
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            if(line.trim().length() == 0 && sb.length() > 30){
	            	paragraphs.add(sb.toString());
	            	sb = new StringBuilder();
	            }
	            line = br.readLine();
	        }
	    } catch (Exception ex){
	    	System.err.println(ex.getMessage());
	    } finally {
	        br.close();
	    }
	    return paragraphs;
	}
	
	private void writeBookContent(List<String> paragraphs, String filePath){
		try{
			FileWriter fstream = new FileWriter(filePath);
			BufferedWriter out = new BufferedWriter(fstream);
			for(String paragraph: paragraphs){
				out.write(paragraph + "\n");				
			}
			out.close();
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
		System.out.println("Done writing: " + filePath);
	}
	
	/**
	 * Get Folder Size
	 * @param folder
	 * @return long size in bytes
	 */
	public static long getSize(File folder) {
		long foldersize = 0;

		File[] filelist = folder.listFiles();
		for (int i = 0; i < filelist.length; i++) {
			if (filelist[i].isDirectory()) {
				foldersize += getSize(filelist[i]);
			} else {
				foldersize += filelist[i].length();
			}
		}
		return foldersize;
	}
	
	public static void main(String[] args) throws IOException{
//		BookDataset bd = new BookDataset(2600,20);
//		bd.execute();
		
		BookPreprocessor bp = new BookPreprocessor();
		bp.preprocessBooks();
	}
}
