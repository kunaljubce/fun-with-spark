from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
	conf = SparkConf().setAppName("analysis_on_retail").setMaster("local[*]")
	sc = SparkContext(conf = conf)

	customers = sc.textFile("/home/hduser/Downloads/analysis_on_retail_data/customers")
	customersRDD = customers.map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2], line.split(",")[5], line.split(",")[6], line.split(",")[7], line.split(",")[8]))
	
	# to find the state-wise distribution of customers i.e. number of customers from each state
	addressRDD = customersRDD.map(lambda k:(k[5],1))
	addressGrouped = addressRDD.reduceByKey(lambda x,y: (x + y))
	addressGroupedSorted = addressGrouped.sortBy(lambda k: k[1], ascending = False)
	addressGroupedSorted.coalesce(1).saveAsTextFile("/home/hduser/Downloads/analysis_on_retail_data/custDistributionByStates")

	# to find all unique customer details for suspected fraud transactions (orders.paymentStatus = SUSPECTED_FRAUD)
	
	
