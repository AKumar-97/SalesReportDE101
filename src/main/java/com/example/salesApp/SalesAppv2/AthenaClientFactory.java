package com.example.salesApp.SalesAppv2;



import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;


public class AthenaClientFactory {

	//in this class we create and configure an amazon athena client to communicate and do stuff 
	// with our athena table
	//It is also responsible for providing us with an instance of the AthenaClient class.

		private final AthenaClientBuilder builder = AthenaClient.builder().region(Region.US_EAST_1).credentialsProvider(EnvironmentVariableCredentialsProvider.create());
		
		public AthenaClient createClient() {
	        return builder.build();
	    }
}
