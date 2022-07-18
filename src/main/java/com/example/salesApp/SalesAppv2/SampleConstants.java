package com.example.salesApp.SalesAppv2;

public class SampleConstants {
	public static final String ATHENA_DEFAULT_DATABASE = "sales-data-supermarket";
	public static final String ATHENA_OUTPUT_S3_FOLDER_PATH = "s3://sales-analytics101/Sample sales report Processed/QueryResults/";
	public static final String ATHENA_QUERY_ONE = "SELECT * FROM \"sales-data-supermarket\".\"sample_sales_report\" limit 10;";
	public static final String ATHENA_QUERY_TWO = "SELECT * FROM \"sales-data-supermarket\".\"income_branch\" limit 10;";
	public static final String ATHENA_QUERY_THREE = "SELECT * FROM \"sales-data-supermarket\".\"income_branch_sex\" limit 10;";
	public static final String ATHENA_QUERY_FOUR = "SELECT * FROM \"sales-data-supermarket\".\"income_branch_product\";";
	public static final String ATHENA_QUERY_FIVE = "SELECT * FROM \"sales-data-supermarket\".\"income_branch_monthly_avg\";";
	public static final String ATHENA_QUERY_SIX = "SELECT * FROM \"sales-data-supermarket\".\"income_hour_max\";";
	public static final long SLEEP_AMOUNT_IN_MS = 1000;
}	
