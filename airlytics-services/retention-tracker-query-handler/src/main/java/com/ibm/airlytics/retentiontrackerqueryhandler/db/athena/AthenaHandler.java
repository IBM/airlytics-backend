package com.ibm.airlytics.retentiontrackerqueryhandler.db.athena;


import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.TableViewConfig;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Component
@DependsOn({"ConfigurationManager", "Airlock"})
public class AthenaHandler {

	//TODO: add timeout
	public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
	public static final long SLEEP_AMOUNT_IN_MS = 1000;
	private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(AthenaHandler.class.getName());

	private Region region;
	private String outputBucket;
	private String catalog;
	private String athenaDB;
	
	public AthenaHandler(ConfigurationManager configurationManager/* String region, String outputBucket, String catalog*/) {
		String region = configurationManager.getAthenaRegion();
		String outputBucket = configurationManager.getAthenaOutputBucket();
		String catalog = configurationManager.getAthenaCatalog();
		String athenaDB = configurationManager.getAthenaDB();

		if (region.equalsIgnoreCase("eu_west_1")) {
			this.region = Region.EU_WEST_1;
		}
		else if (region.equalsIgnoreCase("us_east_1")) {
			this.region = Region.US_EAST_1;
		}
		else {
			throw new RuntimeException("unsupported athena region: " + region + ". Currently only eu_west_1 and us_east_1 are supported");
		}
		//this.region = Region.of(region);
		this.outputBucket = "s3://"+outputBucket;
		this.catalog = catalog;
		this.athenaDB = athenaDB;
	}

	// Submits a sample query to Amazon Athena and returns the execution ID of the query
	private String submitAthenaQuery(AthenaClient athenaClient, String cmd, String athenaDB) throws AthenaException {
		try {
			// The QueryExecutionContext allows us to set the database
			QueryExecutionContext queryExecutionContext = null;
			if (athenaDB!=null) {
				queryExecutionContext = QueryExecutionContext.builder()
						.catalog(catalog)
						.database(athenaDB).build();
			}
			else {
				queryExecutionContext = QueryExecutionContext.builder().catalog(catalog).build();
			}
			
			// The result configuration specifies where the results of the query should go
			ResultConfiguration resultConfiguration = ResultConfiguration.builder()
					.outputLocation(outputBucket)
					.build();

			StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
					.queryString(cmd)
					.queryExecutionContext(queryExecutionContext)
					.resultConfiguration(resultConfiguration)
					.build();

			StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
			return startQueryExecutionResponse.queryExecutionId();

		} catch (AthenaException e) {
			e.printStackTrace();
			throw e;
		}
	}

	// Wait for an Amazon Athena query to complete, fail or to be cancelled
	private void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
		GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
				.queryExecutionId(queryExecutionId).build();

		GetQueryExecutionResponse getQueryExecutionResponse;
		boolean isQueryStillRunning = true;
		while (isQueryStillRunning) {
			getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
			String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
			if (queryState.equals(QueryExecutionState.FAILED.toString())) {
				throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
						.queryExecution().status().stateChangeReason());
			} else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
				throw new RuntimeException("The Amazon Athena query was cancelled.");
			} else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
				isQueryStillRunning = false;
			} else {
				// Sleep an amount of time before retrying again
				Thread.sleep(SLEEP_AMOUNT_IN_MS);
			}
			//System.out.println("The current status is: " + queryState);
		}
	}

	private GetQueryResultsIterable getResultIterable(AthenaClient athenaClient, String queryExecutionId) {
		// Max Results can be set but if its not set,
		// it will choose the maximum page size
		GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
				.queryExecutionId(queryExecutionId)
				.build();

		GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
		return getQueryResultsResults;
	}
	private AthenaQueryResult processResultRows(AthenaClient athenaClient, String queryExecutionId) {

		try {

			// Max Results can be set but if its not set,
			// it will choose the maximum page size
			GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
					.queryExecutionId(queryExecutionId)
					.build();

			GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
			boolean isFirst = true;
			AthenaQueryResult toRet = new AthenaQueryResult();
			for (GetQueryResultsResponse result : getQueryResultsResults) {
				List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
				List<Row> results = result.resultSet().rows();
				if (isFirst) {
					toRet.setColumnInfoList(columnInfoList);
					results = removeHeaderRow(results,columnInfoList);
					toRet.setResults(results);
				} else {
					toRet.getResults().addAll(results);
				}
				isFirst = false;
			}
			return toRet;

		} catch (AthenaException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private List<Row> removeHeaderRow(List<Row> results, List<ColumnInfo> columnInfoList) {
		if (results.size() <= 0) return results;
		Row first = results.get(0);
		List<Datum> allData = first.data();
		boolean equals = true;
		for (int i=0; i<columnInfoList.size();++i) {
			ColumnInfo currInfo = columnInfoList.get(i);
			String currName = currInfo.name();
			Datum currData = allData.get(i);
			String currVal = currData.varCharValue();
			if (!currName.equalsIgnoreCase(currVal)) {
				equals = false;
				break;
			}
		}
		if (equals) {
			List<Row> toRet = new ArrayList<>(results);
			toRet.remove(0);
			return toRet;
		}
		return results;
	}

	private static void processRow(List<Row> row, List<ColumnInfo> columnInfoList) {

		for (Row myRow : row) {
			List<Datum> allData = myRow.data();
			for (Datum data : allData) {
				System.out.println("The value of the column is "+data.varCharValue());
			}
		}
	}


	public GetQueryResultsIterable getDbRecordsForAllTable(String schemaName, String tableName, String orderByField, AthenaClient athenaClient) {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String selectT = dumper.getConfiguration().getString("queryForTable");
		String select = String.format(selectT, tableName, orderByField);
		return readAthenaQuery(select, this.athenaDB, athenaClient);
	}

	public String createAthenaTable(String schemaName, String tableName, String day, String fieldsConfig, String location) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String tableNameTemplate = dumper.getConfiguration().getString("athenaTableName");
		String athenaTableName = String.format(tableNameTemplate, schemaName, tableName,day);
		String createTemplate = dumper.getConfiguration().getString("createTableFromSnapshot");
		String locationTemplate = dumper.getConfiguration().getString("snapshotPathTemplate");
		String locationPath = location+String.format(locationTemplate, schemaName, tableName);
		String createCommand = String.format(createTemplate, athenaTableName, fieldsConfig, locationPath);
		performCreateQuery(createCommand, this.athenaDB);
		return athenaTableName;
	}

	public String createAthenaView(String schemaName, String viewName, Map<String, TableViewConfig> tables, String day, String fieldsConfig, String location) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String viewNameTemplate = dumper.getConfiguration().getString("athenaTableName");
		String athenaTableName = String.format(viewNameTemplate, schemaName, viewName,day);
		String createTemplate = dumper.getConfiguration().getString("createViewFromTablesWithSelect");
		if (tables.size() != 2) {
			logger.error("cannot create view with more/less than 2 tables");
			throw new RuntimeException("cannot create view with more/less than 2 tables");
		}
		String firstTable = new ArrayList<>(tables.keySet()).get(0);
		TableViewConfig firstConfig = tables.get(firstTable);
		String firstColumn = firstConfig.getIdColumn();
		String secondTable = new ArrayList<>(tables.keySet()).get(1);
		TableViewConfig secondConfig = tables.get(secondTable);
		String secondColumn = secondConfig.getIdColumn();
		String selectPart = createViewSelect(firstConfig.getColumns(), secondConfig.getColumns());
		String createCommand = String.format(createTemplate, athenaTableName, selectPart, firstTable, secondTable, firstColumn, secondColumn);
		performCreateQuery(createCommand, this.athenaDB);
		return athenaTableName;
	}

	public String createAthenaViewForDay(String schemaName, String viewName, Map<String, TableViewConfig> tables, String day, String fieldsConfig, String location) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String viewNameTemplate = dumper.getConfiguration().getString("athenaTableName");
		String athenaTableName = String.format(viewNameTemplate, schemaName, viewName,day);
		String createTemplate = dumper.getConfiguration().getString("createViewFromTablesWithSelectForDay");
		if (tables.size() != 2) {
			logger.error("cannot create view with more/less than 2 tables");
			throw new RuntimeException("cannot create view with more/less than 2 tables");
		}
		String firstTable = new ArrayList<>(tables.keySet()).get(0);
		TableViewConfig firstConfig = tables.get(firstTable);
		String firstColumn = firstConfig.getIdColumn();
		String secondTable = new ArrayList<>(tables.keySet()).get(1);
		TableViewConfig secondConfig = tables.get(secondTable);
		String secondColumn = secondConfig.getIdColumn();
		String selectPart = createViewSelect(firstConfig.getColumns(), secondConfig.getColumns());
		String createCommand = String.format(createTemplate, athenaTableName, selectPart, firstTable, day, secondTable, day, firstColumn, secondColumn);
		performCreateQuery(createCommand, this.athenaDB);
		return athenaTableName;
	}

	public String repairAthenaTableMultiSharded(String tableName, int minShard, int maxShard, String location, String select, String orderByField, String day) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String convertTemplate = dumper.getConfiguration().getString("convertRepairTableMultiShard");
		String convertCmd = String.format(convertTemplate, tableName, minShard, maxShard, location, select, tableName, minShard, maxShard,day, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableNameMultiShard");
		String convertedTableName = String.format(tableNameT, tableName, minShard, maxShard);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}
	public String splitAthenaTableMultiSharded(String tableName, int minShard, int maxShard, String location, String select, String orderByField, String day, String application, String platform) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String convertTemplate = dumper.getConfiguration().getString("convertSplitTableMultiShard");
		String convertCmd = String.format(convertTemplate, tableName+day, application, minShard, maxShard, location, select, tableName, minShard, maxShard,day,platform, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedSplitTableNameMultiShard");
		String convertedTableName = String.format(tableNameT, tableName+day,application, minShard, maxShard);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}
	public String repairAthenaTableShardNull(String tableName, String location, String select, String orderByField, String day) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String convertTemplate = dumper.getConfiguration().getString("convertRepairTableForShardNull");
		String convertCmd = String.format(convertTemplate, tableName, location, select, tableName,day, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableNameShardNull");
		String convertedTableName = String.format(tableNameT, tableName);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}
	public String splitAthenaTableShardNull(String tableName, String location, String select, String orderByField, String day, String application, String platform) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String convertTemplate = dumper.getConfiguration().getString("convertSplitTableForShardNull");
		String convertCmd = String.format(convertTemplate, tableName+day, application, location, select, tableName,day, platform, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedSplitTableNameShardNull");
		String convertedTableName = String.format(tableNameT, tableName+day, application);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}

	private String createViewSelect(List<String> columnsA, List<String> columnsB) {
		if (columnsA==null && columnsB==null) {
			return "*";
		}
		StringJoiner sj = new StringJoiner(",");
		if (columnsA!=null) {
			for (String column : columnsA) {
				sj.add("\"a\".\""+column+"\"");
			}
		} else {
			sj.add("\"a\".*");
		}
		if (columnsB!=null) {
			for (String column : columnsB) {
				sj.add("\"b\".\""+column+"\"");
			}
		}  else {
			sj.add("\"b\".*");
		}
		return sj.toString();
	}

	public void dropTable(String tableName) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return;
		}
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, tableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
	}
	public void dropView(String viewName) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return;
		}
		String dropV = dumper.getConfiguration().getString("dropView");
		String dropViewCmd = String.format(dropV, viewName);
		performCreateQuery(dropViewCmd, this.athenaDB);
	}
	public String convertAthenaTableForShardNull(String athenaTableName, int shard, String location, String select, String orderByField, String schemaName, String tableName) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String bucketByField = orderByField;
		String convertTemplate = dumper.getConfiguration().getString("convertSnapshotTableForNullShard");
		String convertCmd = String.format(convertTemplate, tableName, shard, bucketByField, location, select, athenaTableName, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableName");
		String convertedTableName = String.format(tableNameT, tableName, shard);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}
	public String convertAthenaTableMultiSharded(String tableName, int minShard, int maxShard, String location, String select, String orderByField) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String bucketByField = orderByField;
		String convertTemplate = dumper.getConfiguration().getString("convertSnapshotTableMultiShard");
		String convertCmd = String.format(convertTemplate, tableName, minShard, maxShard, bucketByField, location, select, tableName, minShard, maxShard, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableNameMultiShard");
		String convertedTableName = String.format(tableNameT, tableName, minShard, maxShard);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;
	}
	public String convertAthenaTable(String tableName, int shard, String location, String select, String orderByField, String schema, String table) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
//		String locationTemplate = dumper.getConfiguration().getString("convertedPathTemplate");
//		String location2 = String.format(locationTemplate, snapshotPath, schema, table, shard);
		String convertTemplate = dumper.getConfiguration().getString("convertSnapshotTable");
		String convertCmd = String.format(convertTemplate, tableName, shard, location, select, tableName, shard, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableName");
		String convertedTableName = String.format(tableNameT, tableName, shard);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;

	}

	public String convertAthenaTableNotSharded(String tableName, int shard, String location, String select, String orderByField, String schema, String table, String day) throws InterruptedException {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String bucketByField = orderByField;
		String convertTemplate = dumper.getConfiguration().getString("convertSnapshotTableNotSharded");
		String convertCmd = String.format(convertTemplate, tableName+day, bucketByField, location, select, tableName, orderByField);
		performCreateQuery(convertCmd, this.athenaDB);
		//delete table
		String tableNameT = dumper.getConfiguration().getString("convertedTableNameNotSharded");
		String convertedTableName = String.format(tableNameT, tableName+day);
		String dropT = dumper.getConfiguration().getString("dropTable");
		String dropTableCmd = String.format(dropT, convertedTableName);
		performCreateQuery(dropTableCmd, this.athenaDB);
		return location;

	}

	public GetQueryResultsIterable getDbRecordsForShards(String schemaName, String tableName, int minShard, int maxShard, AthenaClient athenaClient) {
		Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.ATHENA_QUERIES);
		if (!dumper.isOn() || dumper.getConfiguration()==null) {
			logger.error("feature "+AirlockConstants.query.ATHENA_QUERIES+" is off");
			return null;
		}
		String selectT = dumper.getConfiguration().getString("shardedQueryWithStepsForTable");
		String select = String.format(selectT, tableName, minShard, maxShard);
		return readAthenaQuery(select, this.athenaDB, athenaClient);
	}



    public static class ColumnData {
		public String columnName;
		public String columnType;
		public Boolean isPartitionKey;
	}

	public AthenaClient getAthenaClient() {
		AthenaClient athenaClient = AthenaClient.builder()
				.region(region)
				.build();
		return athenaClient;
	}
	private void performCreateQuery(String cmd, String athenaDb) throws InterruptedException {
		AthenaClient athenaClient = AthenaClient.builder()
				.region(region)
				.build();
		try {
			String queryExecutionId = submitAthenaQuery(athenaClient, cmd, athenaDb);
			waitForQueryToComplete(athenaClient, queryExecutionId);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw  e;
		} finally {
			athenaClient.close();
		}
	}
	public GetQueryResultsIterable readAthenaQuery(String cmd, String athenaDb, AthenaClient athenaClient) {

		try {
			String queryExecutionId = submitAthenaQuery(athenaClient, cmd, athenaDb);
			waitForQueryToComplete(athenaClient, queryExecutionId);
//			return processResultRows(athenaClient, queryExecutionId);
			return getResultIterable(athenaClient, queryExecutionId);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
//			athenaClient.close();
		}


		return null;
	}
	public ArrayList<ColumnData> getColumnData(String athenaDB, String athenaTable) throws InterruptedException {
		ArrayList<ColumnData> res = new ArrayList<>();
		AthenaClient athenaClient = AthenaClient.builder()
				.region(region)
	            .build();
		
		String command = "SELECT column_name, data_type, extra_info FROM information_schema.columns WHERE table_schema = '" + athenaDB + "' and table_name = '"+ athenaTable + "'";
		String queryExecutionId = submitAthenaQuery(athenaClient, command, athenaDB);
		waitForQueryToComplete(athenaClient, queryExecutionId);
	    
		GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
				.queryExecutionId(queryExecutionId)
				.build();
	
		GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
	
		for (GetQueryResultsResponse result : getQueryResultsResults) {
			List<Row> results = result.resultSet().rows();
			for (Row row : results) {
				List<Datum> allData = row.data();
				int i=0;
				ColumnData cd = null;
				for (Datum data : allData) {
					String val = data.varCharValue();
					if (i==0 && val.equals("column_name")) {
						break;
					}
					if (i==0) {
						cd = new ColumnData();
						cd.columnName = val;
					}
					else if (i==1){
						cd.columnType = val;
					}
					else if (i==2){
						cd.isPartitionKey = val!=null&&val.equalsIgnoreCase("partition key");
					}
					i++;
					//System.out.println(data.varCharValue());
				}
				if (cd!=null) {
					res.add(cd);
				}
			}
		}
		
	    athenaClient.close();
	    return res;
	}
	public ArrayList<String> getAthenaTableColumns(String athenaTable, String athenaDB) throws InterruptedException {
		String cmd = "SHOW COLUMNS IN " + athenaTable +";";
		return getResultsAsStringsList(cmd, athenaDB);	
	}
	
	public ArrayList<String> getAthenaTablesInDatabase(String athenaDB) throws InterruptedException {
		String cmd = "SHOW TABLES IN " + athenaDB +";";
		return getResultsAsStringsList(cmd, null);	
	}
	
	public ArrayList<String> getAthenaDatabases() throws InterruptedException {
		String cmd = "SHOW DATABASES;";
		return getResultsAsStringsList(cmd, null);	
	}
	
	private ArrayList<String> getResultsAsStringsList (String command, String athenaDB) throws InterruptedException {
		ArrayList<String> res = new ArrayList<>();
		AthenaClient athenaClient = AthenaClient.builder()
				.region(region)
                .build();
		
		String queryExecutionId = submitAthenaQuery(athenaClient, command, athenaDB);
		waitForQueryToComplete(athenaClient, queryExecutionId);
        
		GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
				.queryExecutionId(queryExecutionId)
				.build();

		GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

		for (GetQueryResultsResponse result : getQueryResultsResults) {
			List<Row> results = result.resultSet().rows();
			for (Row row : results) {
				List<Datum> allData = row.data();
				for (Datum data : allData) {
					res.add(data.varCharValue().trim());
				}
			}
		}
		
        athenaClient.close();
        return res;
	}
/*
	// Retrieves the results of a query
	private void processResultRows(AthenaClient athenaClient, String queryExecutionId) {

		try {
			GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
					.queryExecutionId(queryExecutionId)
					.build();

			GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

			for (GetQueryResultsResponse result : getQueryResultsResults) {
				List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
				List<Row> results = result.resultSet().rows();
				processRow(results, columnInfoList);
			}

		} catch (AthenaException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void processRow(List<Row> row, List<ColumnInfo> columnInfoList) {

		for (Row myRow : row) {
			List<Datum> allData = myRow.data();
			for (Datum data : allData) {
				System.out.println("The value of the column is "+data.varCharValue());
			}
		}
	}*/
}



