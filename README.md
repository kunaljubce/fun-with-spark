# hadoop-and-pyspark-projects
This is a placeholder for all my Hadoop projects including PySpark, Hive, Kafka etc. The files and folders are named in a way that helps identifying the corresponding code and data easily. Forexample, for the code transactions.py, there is a folder named transactions under /data which contains all necessary datasets used for the code.

Project 1 - To identify:
		a) All transactions for each account that have the same transaction_source and same transaction_amount in both months
		b) All accounts that have transactions in either month but not both.
The datasets are for the months August and September and have 2000 records each. The code is written in PySpark. The logic is implemented using RDDs, without resorting to the use of DataFrames.

Project 2 - This project is done to analyse the data for a large retail chain based in US. The purpose of this project is to identify the KPIs and analyse them to help reduce fraud and also get deeper business insights. For this reason, we have taken into account a subset of the orders data, products data, customers data and departments data.
No Personally Indentifiable Information (PII) has been used to analyse the datasets. We are trying to determine the following:
		- find the state-wise distribution of customers i.e. number of customers from each state
		- find all unique customer details for suspected fraud transactions (orders.paymentStatus = SUSPECTED_FRAUD)
		- find total amount and total number of orders for each of value of paymentStatus
		- find number of products that are from the brands - Nike, Adidas, Reebok, Puma, Majestic, Under Armour and Fitbit
		- find the number of products that below to different price ranges i.e. $0-$20, $20-$40 etc. Use dataframes for this.
		- find the productId, productDescription and price of the top 20 highest and lowest selling items
