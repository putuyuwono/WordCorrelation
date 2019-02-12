package dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
/**
 * This class will be used for connecting our apps to Yohanes' MySql Database Server
 * @author hduser
 *
 */
public class MySQLConnector {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	private String user = "root";
	private String pass = "root";
	private String url = "jdbc:mysql://node05:3306/biography";

	public ResultSet executeQuery(String query) throws Exception {

		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager.getConnection(url, user, pass);
			// Statements allow to issue SQL queries to the database
			statement = connect.createStatement();
			// Result set get the result of the SQL query
			resultSet = statement.executeQuery(query);
			connect.close();
		} catch (Exception e) {
			throw e;
		} 
		return resultSet;
	}
	
	public int executeInsert(String insertQuery, String[] params){
		int affectedRows = 0;
		try{
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager.getConnection(url, user, pass);
			// Statements allow to issue SQL queries to the database
			preparedStatement = connect.prepareStatement(insertQuery);
			
			for(int i=1; i<=params.length; i++){
				String value = params[i-1];
				preparedStatement.setString(i, value);
			}
			affectedRows = preparedStatement.executeUpdate();
			connect.close();
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		return affectedRows;
	}

	// You need to close the resultSet
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {

		}
	}

}
