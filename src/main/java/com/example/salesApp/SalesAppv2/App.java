package com.example.salesApp.SalesAppv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.web.JsonPath;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.athena.model.QueryExecutionState;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App
{
	private static final String ATHENA_DATABASE = "sales-data-supermarket";
	private static final String ATHENA_OUTPUT_S3_FOLDER_PATH = "s3://sales-analytics101/Sample sales report Processed/QueryResults/";
	public static void main( String[] args )
    {
    	SpringApplication.run(App.class, args);
    }
	
	/*
	 * @Override public void run(String... args)throws Exception{ //This function
	 * will be run when SpringApplication.run is called AthenaClientFactory factory
	 * = new AthenaClientFactory(); AthenaClient athenaClient =
	 * factory.createClient();
	 * 
	 * //Now we shall call the submitAthenaQuery function to submit and execute the
	 * sql query in athena String queryExecutionID =
	 * submitAthenaQuery(athenaClient);
	 * 
	 * //Now we wait for the query to complete waitForQuerytoComplete(athenaClient,
	 * queryExecutionID); processResultsRows(athenaClient, queryExecutionID);
	 * athenaClient.close(); }
	 */
	
	//function to submit query using athena client
	public static String submitAthenaQuery(AthenaClient athenaClient, String query) 
	{
			//First we set the database. 
			// We use "QueryExecutionContext" for the same
			QueryExecutionContext qryExecutionContext = QueryExecutionContext.builder().database(SampleConstants.ATHENA_DEFAULT_DATABASE).build();
			//Now we specify where the result should go to.
			ResultConfiguration resultConfig = ResultConfiguration.builder().outputLocation(SampleConstants.ATHENA_OUTPUT_S3_FOLDER_PATH).build();
			
			StartQueryExecutionRequest startQryExecReq = StartQueryExecutionRequest.builder()
					.queryString(query/* SampleConstants.ATHENA_QUERY_THREE */)
					.queryExecutionContext(qryExecutionContext)
					.resultConfiguration(resultConfig)
					.build();
			
			StartQueryExecutionResponse startQryExecRes = athenaClient.startQueryExecution(startQryExecReq);
			return startQryExecRes.queryExecutionId();
		
	}
	
	//function to wait for athena to complete processing the query, fail or be cancelled
	public static void waitForQuerytoComplete(AthenaClient athenaClient, String queryExecutionID) throws InterruptedException
	{
		GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
				.queryExecutionId(queryExecutionID).build();
		GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        
        while(isQueryStillRunning) {
        	getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
        	String queryState = getQueryExecutionResponse.queryExecution()
        			.status().state().toString();
        	if(queryState.equals(QueryExecutionState.FAILED.toString())) {
        		throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
        	}
        	else if(queryState.equals(QueryExecutionState.CANCELLED.toString())) {
        		throw new RuntimeException("The Amazon Athena query was cancelled.");
        	}
        	else if(queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
        		isQueryStillRunning = false;
        	}
        	else {
        		Thread.sleep(SampleConstants.SLEEP_AMOUNT_IN_MS);
        	}
        	System.out.println("The current status is:" + queryState);
        }
	}
	
	
	//This function retrieves the results of the query
	public static void processResultsRows(AthenaClient athenaClient, String queryExecutionID) {
		GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
				.queryExecutionId(queryExecutionID).build();
		
		GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
		List<Row> results = null;
		
		for(GetQueryResultsResponse Resultresult : getQueryResultsResults) {
			List<ColumnInfo> columnInfoList = Resultresult.resultSet().resultSetMetadata().columnInfo();
			results = Resultresult.resultSet().rows();
			/* return results; */
			 processRow(results, columnInfoList);
		}
		
	}
	
	//This function is used to process each row
	public static void processRow(List<Row> rowList, List<ColumnInfo> columnInfoList) {
		List<String> columns = new ArrayList<>();
		for(ColumnInfo columnInfo : columnInfoList) {
			columns.add(columnInfo.name());
		}
		for (Row myRow : rowList) {
			//hashmap with the name of the column as key; and populate datum value to corresponding key(column name)
			//object mapper from jackson; spring jackson
            List<Datum> allData = myRow.data();
            //this list contains the data returned by the athena query
            //this data is being printed row-wise
            //thus we will try to use hashmap to store the data 
            for (Datum data : allData) {
                System.out.println("The value of the column is "+data.varCharValue());
            }
        }
	}
	
}
	
