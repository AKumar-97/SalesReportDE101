package com.example.salesApp.SalesAppv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MultiValuedMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Row;

@RestController
@RequestMapping(path = "/salesReport")
public class SalesService {
	//first endpoint to display first 10 rows of raw data
	@GetMapping("/query01")
	public String getQuery01() throws InterruptedException, JsonProcessingException {
		String query = SampleConstants.ATHENA_QUERY_ONE;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();;
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
        athenaClient.close();
		return (map.writeValueAsString(listOfResult));
	}
	//second endpoint to display total sales for each branch
	@GetMapping("/query02")
	public String getQuery02() throws InterruptedException, JsonProcessingException {
		String query = SampleConstants.ATHENA_QUERY_TWO;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		System.out.println("Total sale for each branch");
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();;
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
        athenaClient.close();
		return (map.writeValueAsString(listOfResult));
	}
	
	//third endpoint to display total sales per branch for each sex
	@GetMapping("/query03")
	public String getQuery03() throws InterruptedException, JsonProcessingException {
		String query = SampleConstants.ATHENA_QUERY_THREE;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		System.out.println("Total sale for each branch per gender");
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();;
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
        athenaClient.close();
		return (map.writeValueAsString(listOfResult));
	}
	
	//fourth endpoint to display total sales per branch for each product type
	@GetMapping("/query04")
	public String getQuery04() throws InterruptedException, JsonProcessingException {
		String query = SampleConstants.ATHENA_QUERY_FOUR;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		System.out.println("Total sale per branch for each type of product");
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();;
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
        athenaClient.close();
		return (map.writeValueAsString(listOfResult));
	}
	
	//fifth endpoint to display average monthly sales per branch
	@GetMapping("/query05")
	public String getQuery05() throws InterruptedException, JsonProcessingException {
		String query = SampleConstants.ATHENA_QUERY_FIVE;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		System.out.println("Average monthly sale for each branch");
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();;
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
        athenaClient.close();
		return (map.writeValueAsString(listOfResult));
		/* return resultData; */
	}
	
	//sixth endpoint to display max sale per hour irrespective of day, month or branch
	@GetMapping("/query06")
	public String getQuery06() throws InterruptedException, JsonProcessingException{
		String query = SampleConstants.ATHENA_QUERY_SIX;
		AthenaClientFactory factory = new AthenaClientFactory();
		AthenaClient athenaClient = factory.createClient();
			
		//Now we shall call the submitAthenaQuery function to submit and execute the sql query in athena
		System.out.println("Busiest hour of the day.(Hours with the max sale)");
		String queryExecutionID = App.submitAthenaQuery(athenaClient,query);
		
		//Now we wait for the query to complete
		App.waitForQuerytoComplete(athenaClient, queryExecutionID);
		Map<String, String> resultData = new HashMap<>();
		ArrayList<String> listOfResult = new ArrayList<String>();
		listOfResult = App.processResultsRows(athenaClient, queryExecutionID);
		ObjectMapper map = new ObjectMapper();
		/* return (map.writeValueAsString(resultData)); */
        athenaClient.close();
        return (map.writeValueAsString(listOfResult));
	}
}
