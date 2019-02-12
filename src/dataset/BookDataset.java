package dataset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BookDataset {
	String DRIVER = "com.mysql.jdbc.Driver";
	String DB_URL = "jdbc:mysql://164.125.121.134/book";
	String USERNAME = "root";
	String PASSWORD = "root";
	
	String outputDir = "books/";
	int offset = 0;
	int bookLimit = 100;
	
	public BookDataset(int numberOfBook){
		this.bookLimit = numberOfBook;
	}
	
	public BookDataset(int offset, int numberOfBook){
		this.offset = offset;
		this.bookLimit = numberOfBook;
	}
	
	public Connection getConnection() throws Exception {
		Class.forName(DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USERNAME,
				PASSWORD);
		return conn;
	}
	
	public void execute() {
		Connection conn = null;
		ResultSet rs;
		try {
			conn = getConnection();
			System.out.println("connected");
			Statement stmt = conn.createStatement();
			String nquery = "SELECT id,content FROM books LIMIT " + offset + "," + bookLimit;
			rs = stmt.executeQuery(nquery);

			while (rs.next()) {
				String id = rs.getString(1);
				String content = rs.getString(2);
				writeOriginal(id, content);
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void writeOriginal(String id, String content){
		String filePath = outputDir + id + ".txt";
		System.out.println("Writing: " + filePath);
		try{
			File file =new File(filePath);
			//if file doesnt exists, then create it
			if(!file.exists()){
				file.createNewFile();
			}

			FileWriter fstream = new FileWriter(filePath,true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			out.write(content);
			out.close();
		}catch(Exception ex){
			System.err.println(ex.getMessage());
		}
	}
	
	public static void main(String[] args){
		BookDataset bd = new BookDataset(5);
		bd.execute();
	}
}
