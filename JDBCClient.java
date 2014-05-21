

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;


@SuppressWarnings("nls")
public class JDBCClient {

	public static void main(String[] args) throws Exception {
		execute(getDriverConnection(args[0]), args[1]);
	}

	static Connection getDriverConnection(String propsFile) throws Exception {
		Class.forName("com.sas.net.sharenet.ShareNetDriver");

		Properties props = new Properties();
		File f = new File(propsFile);
		if (f.exists()) {
		    System.out.println("Using the property file "+f.getName());
		    props.load(new FileInputStream(f));
		}
		else {
		    System.out.println("Property file not found "+f.getName());
		}
		System.out.println("Properties:"+props);
		System.out.println("Trying to Connect...");
		return DriverManager.getConnection(props.getProperty("url"), props);
	}

	public static void execute(Connection connection, String sql) throws Exception {
		try {
		    System.out.println("Connection Sucessful...");
		    System.out.println("Starting to Execute SQL = "+sql);
			Statement statement = connection.createStatement();

			ResultSet results = statement.executeQuery(sql);

			ResultSetMetaData metadata = results.getMetaData();
			int columns = metadata.getColumnCount();
			System.out.println("Results");
			for (int row = 1; results.next(); row++) {
				System.out.print(row + ": ");
				for (int i = 0; i < columns; i++) {
					if (i > 0) {
						System.out.print(",");
					}
					Object obj = results.getObject(i+1);
					System.out.print(obj);
				}
				System.out.println();
			}
			System.out.println("Query Plan");

			results.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	public static void executeCall(Connection connection, String sql) throws Exception {
		try {
			CallableStatement statement = connection.prepareCall(sql);
			//statement.registerOutParameter(1, Types.VARCHAR);

			boolean haveResults = statement.execute();

			if (haveResults) {

				ResultSet results = statement.getResultSet();

				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				System.out.println("Results");
				for (int row = 1; results.next(); row++) {
					System.out.print(row + ": ");
					for (int i = 0; i < columns; i++) {
						if (i > 0) {
							System.out.print(",");
						}
						System.out.print(results.getString(i+1));
					}
					System.out.println();
				}

				results.close();
				statement.close();
			}
			else {
				System.out.println("result="+statement.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	public static void executeUpdate(Connection connection, String csvFile) throws Exception {
		try {
			List<List<String>> rows = new ArrayList<List<String>>();
			String sql = parseCSV(csvFile, rows);
			PreparedStatement statement = connection.prepareStatement(sql);
			for (List<String> row:rows) {
				for (int i = 0; i < row.size(); i++) {
					String value = row.get(i);
					if (value.equals("null")) {
						statement.setObject(i+1, null);
					}
					else {
						statement.setObject(i+1, value);
					}
				}
				statement.addBatch();
			}
			int[] updateCount = statement.executeBatch();
			System.out.println("update-count = ");
			for (int i = 0; i < updateCount.length; i++) {
				System.out.println(updateCount[i]);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	private static String parseCSV(String csvFile, List<List<String>> rows) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(csvFile));
		String sql = reader.readLine();
		try {
			String line = null;
			while((line=reader.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0) {
					List<String> row = new ArrayList();
					StringTokenizer st = new StringTokenizer(line, ",");
					while (st.hasMoreTokens()) {
						row.add(st.nextToken());
					}
					rows.add(row);
				}
			}
		} finally {
			reader.close();
		}
		return sql;
	}


	public static void writeCSV(Connection connection, String sql) throws Exception {
		try {
			Statement statement = connection.createStatement();

			ResultSet results = statement.executeQuery(sql);

			ResultSetMetaData metadata = results.getMetaData();
			int columns = metadata.getColumnCount();

			for (int row = 1; row < columns; row++) {
				System.out.print(metadata.getColumnLabel(row));
				System.out.print(",");
			}
			System.out.println();
			while (results.next()){
				for (int i = 0; i < columns; i++) {
					if (i > 0) {
						System.out.print(",");
					}
					System.out.print(results.getString(i+1));
				}
				System.out.println();
			}
			results.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
