package database.benchmark.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

public class Neo4jQueryExecuter {
	private static final int NUMBER_OF_SCENARIOS = 2;
	private static final String NEO4J_CONF = "neo4j.conf";
	private GraphDatabaseService neo4jGraph = null;
	private String databasePath;
	private ArrayList<String> queryList;
	private Hashtable<String, Long> queryExecutionTime;
	private long totalAvgQueriesExecTime;
	private String queriesPath;
	File file;

	public Neo4jQueryExecuter(String databasePath, String queriesPath) {
		this.setDatabasePath(databasePath);
		this.queriesPath = queriesPath;
		loadDatabase();
		loadQueries();
		totalAvgQueriesExecTime = 0;
	}

	private void loadDatabase() {
		System.out.println(getDatabasePath());
		file = new File(getDatabasePath());

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

	// execute queries permutably
	public void ExecuteQueries() {
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(file)
				.loadPropertiesFromURL(this.getClass().getClassLoader().getResource(NEO4J_CONF)).newGraphDatabase();

		registerShutdownHook(neo4jGraph);
		PermutationIterator<String> permutedQueryList = new PermutationIterator<String>(queryList);
		setQueryExecutionTime(new Hashtable<String, Long>());
		StopWatch watch = new StopWatch();
		Transaction trx = neo4jGraph.beginTx();
		int scenarioNo = 0;
		while (permutedQueryList.hasNext()) {
			watch.start();
			ArrayList<String> queries = (ArrayList<String>) permutedQueryList.next();
			executeQuery(queries);

			if (scenarioNo > 0) {// ignore the first round of queries execution
									// as
									// it's a warm up
				if (scenarioNo > NUMBER_OF_SCENARIOS)
					break;
				totalAvgQueriesExecTime += (long) watch.getTime(TimeUnit.MILLISECONDS);
			}
			queryExecutionTime.put("S" + scenarioNo, (long) watch.getTime(TimeUnit.MILLISECONDS));
			++scenarioNo;
			watch.stop();
			watch.reset();
		}
		trx.close();

		// int noOfConsideredscenarios = scenarioNo - 1;
		// if (noOfConsideredscenarios != 0)
		totalAvgQueriesExecTime = totalAvgQueriesExecTime / (NUMBER_OF_SCENARIOS);

		logQueriesExecutionTime();

	}

	public void ExecuteQueriesSeperately() {
		setQueryExecutionTime(new Hashtable<String, Long>());

		for (int i = 0; i < queryList.size(); i++) {
			executeQueryMultyipleTimes(i, queryList.get(i), NUMBER_OF_SCENARIOS);
		}

		logQueriesExecutionTime();

	}

	private void executeQueryMultyipleTimes(int i, String query, int numberOfScenarios) {
		neo4jGraph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(file)
				.loadPropertiesFromURL(this.getClass().getClassLoader().getResource(NEO4J_CONF)).newGraphDatabase();

		registerShutdownHook(neo4jGraph);
		Transaction trx = neo4jGraph.beginTx();
		StopWatch watch = new StopWatch();
		Result result = neo4jGraph.execute(query);
		// System.out.println(queryList.get(i));
		System.out.println(result.resultAsString());

		watch.start(); // start watch after the first warm up execution
		for (int j = 1; j < numberOfScenarios; j++) {
			result = neo4jGraph.execute(query);
			// System.out.println(queryList.get(i));
			System.out.println(result.resultAsString());

		}

		long time = (long) watch.getTime(TimeUnit.MILLISECONDS) / (NUMBER_OF_SCENARIOS - 1);
		queryExecutionTime.put("Q" + (i + 1),
				time);
		System.out.println(">>>>>>> time:: " + time);
		watch.stop();
		watch.reset();
		trx.close();
		neo4jGraph.shutdown();

	}

	private void logQueriesExecutionTime() {
		String fileName = getDatabaseName() + "_" + queriesPath + "WZout_index.log";
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
//			System.out.println("TotalAvgExecTime ====> " + totalAvgQueriesExecTime);
			bufferedWriter.flush();
			fileWriter.flush();
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public int calculateHits(List<ExecutionPlanDescription> list) {
		int hits = 0;
		int head = 0;
		if (list.isEmpty())
			return 0;
		// System.out.println("list size >>> " + list.size());
		System.out.println(list.get(head).getName());
		// System.out.println( list.get(head).getName() + " >>> childern size
		// >>> " + list.get(head).getChildren().size());
		if (list.get(head).hasProfilerStatistics()) {
			hits += list.get(head).getProfilerStatistics().getDbHits();
			// System.out.println(hits);
		}
		hits += calculateHits(list.get(head).getChildren()); // recurse over the
																// children of
																// the head
		list.remove(head); // remove the head to recurse on the remaining of the
							// list
		hits += calculateHits(list);
		// System.out.println(hits);
		return hits;
	}

	private String getDatabaseName() {
		return databasePath.substring(databasePath.lastIndexOf("\\") + 1);
	}

	private void executeQuery(ArrayList<String> queryList) {
		for (int i = 0; i < queryList.size(); i++) {
			// TODO: log result
			Result result = neo4jGraph.execute(queryList.get(i));
			// System.out.println(queryList.get(i));
			System.out.println(result.resultAsString());

			int hits = calculateHits(result.getExecutionPlanDescription().getChildren());
			System.out.println(">>>>>> DB Hits = " + hits);
		}

	}

	public String getDatabasePath() {
		return databasePath;
	}

	public void setDatabasePath(String databasePath) {
		this.databasePath = databasePath;
	}

	public long getTotalAvgQueriesExecTime() {
		return totalAvgQueriesExecTime;
	}

	public void setTotalAvgQueriesExecTime(long totalAvgQueriesExecTime) {
		this.totalAvgQueriesExecTime = totalAvgQueriesExecTime;
	}

	public Hashtable<String, Long> getQueryExecutionTime() {
		return queryExecutionTime;
	}

	public void setQueryExecutionTime(Hashtable<String, Long> queryExecutionTime) {
		this.queryExecutionTime = queryExecutionTime;
	}

	public static void main(String[] args) {

	
		Neo4jQueryExecuter executer = new Neo4jQueryExecuter(args[0], args[1]); // path to the neo4j database and path to the queries file 
		executer.ExecuteQueriesSeperately();
		

	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
