package cl.cam.ac.provenance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.iterators.PermutationIterator;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class PostgreQueryExecuter {
	private static final int NUMBER_OF_SCENARIOS = 4;
	private ArrayList<String> queryList;
	private Hashtable<String, Long> queryExecutionTime;
	private long totalAvgQueriesExecTime;
	private String queriesPath;
	Connection connection = null;

	public PostgreQueryExecuter(String queriesPath) {

		this.queriesPath = queriesPath;

		loadQueries();
		totalAvgQueriesExecTime = 0;
	}

	private void loadDatabase() {

		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "admin");
			connection.setAutoCommit(false);
			System.out.println("Opened database successfully");

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

	}

	private void closeDatabase() {

		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// read queries from file
	public void loadQueries() {
		FileReader fileReadr;
		queryList = new ArrayList<String>();

		try {
			System.out.println(this.getClass().getClassLoader().getResource(queriesPath).getPath());
			fileReadr = new FileReader(this.getClass().getClassLoader().getResource(queriesPath).getPath());
			BufferedReader bufferedReader = new BufferedReader(fileReadr);
			String line = bufferedReader.readLine();

			while (line != null) {
				System.out.println(line);
				queryList.add(line);
				line = bufferedReader.readLine();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void ExecuteQueriesSeperately() {
		setQueryExecutionTime(new Hashtable<String, Long>());

		for (int i = 0; i < queryList.size(); i++) {
			executeQueryMultyipleTimes(i, queryList.get(i), NUMBER_OF_SCENARIOS);
		}

		logQueriesExecutionTime();

	}

	private void executeQueryMultyipleTimes(int i, String query, int numberOfScenarios) {
		try {
			loadDatabase();
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			StopWatch watch = new StopWatch();

			watch.start(); // start watch after the first warm up execution
			for (int j = 1; j < numberOfScenarios; j++) {
				System.out.println("----------------------------------------------------");
				rs = stmt.executeQuery(query);
				printResult(rs);
			}
			long time = (long) watch.getTime(TimeUnit.MILLISECONDS) / (NUMBER_OF_SCENARIOS - 1);
			queryExecutionTime.put("Q" + (i + 1),
					time);
			System.out.println(">>>>>>> time:: " + time);
			watch.stop();
			watch.reset();
			rs.close();
			stmt.close();
			closeDatabase();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void printResult(ResultSet resultSet) {
		try {
			ResultSetMetaData rsmd;
			rsmd = resultSet.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (resultSet.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1)
						System.out.print(",  ");
					String columnValue = resultSet.getString(i);
					System.out.print(columnValue + " " + rsmd.getColumnName(i));
				}
				System.out.println("");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void logQueriesExecutionTime() {
		String fileName = "Pstgre_" + queriesPath + ".log";
		System.out.println(fileName);
		// write execution times to file named after db name+ q file name
		try {
			;
			FileWriter fileWriter = new FileWriter(fileName);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			Iterator it = queryExecutionTime.entrySet().iterator();
			Map.Entry pair = null;
			while (it.hasNext()) {
				pair = (Map.Entry) it.next();
				System.out.println(pair.getKey() + " ====> " + pair.getValue());
				bufferedWriter.write(pair.getKey() + " ====> " + pair.getValue() + "\n");
			}
			bufferedWriter.write("TotalAvgExecTime ====> " + totalAvgQueriesExecTime + "\n");
			// System.out.println("TotalAvgExecTime ====> " +
			// totalAvgQueriesExecTime);
			bufferedWriter.flush();
			fileWriter.flush();
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Hashtable<String, Long> getQueryExecutionTime() {
		return queryExecutionTime;
	}

	public void setQueryExecutionTime(Hashtable<String, Long> queryExecutionTime) {
		this.queryExecutionTime = queryExecutionTime;
	}

	public static void main(String[] args) {

		PostgreQueryExecuter executer = new PostgreQueryExecuter(args[0]); //path to the queries file
		executer.ExecuteQueriesSeperately();

	}

}
