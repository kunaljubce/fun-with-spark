from pyspark import SparkConf, SparkContext, StorageLevel

if __name__ == "__main__":
	conf = SparkConf().setAppName("analysis_on_retail").setMaster("local[*]")
	sc = SparkContext(conf = conf)

	customers = sc.textFile("/home/hduser/Downloads/analysis_on_retail_data/customers")
	customersRDD = customers.map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2], line.split(",")[5], line.split(",")[6], line.split(",")[7], line.split(",")[8]))
	customersRDD.persist(StorageLevel.MEMORY_ONLY)
	
	#1 to find the state-wise distribution of customers i.e. number of customers from each state
	addressRDD = customersRDD.map(lambda k:(k[5],1))
	addressGrouped = addressRDD.reduceByKey(lambda x,y: (x + y))
	addressGroupedSorted = addressGrouped.sortBy(lambda k: k[1], ascending = False)
	addressGroupedSorted.coalesce(1).saveAsTextFile("/home/hduser/Downloads/analysis_on_retail_data/custDistributionByStates")

	#2 to find all unique customer details for suspected fraud transactions (orders.paymentStatus = SUSPECTED_FRAUD)
	orders = sc.textFile("/home/hduser/Downloads/analysis_on_retail_data/orders")
	ordersSuspectedFraud = orders.filter(lambda k:(k.split(",")[3] == "SUSPECTED_FRAUD"))
	# creating pair RDDs for orders and customers based on the customerId as the key in each pair RDD
	ordersSuspectedFraudPairRDD = ordersSuspectedFraud.map(lambda k: (k.split(",")[2], (k.split(",")[0], k.split(",")[1], k.split(",")[3])))
	ordersSuspectedFraudPairRDDGrouped = ordersSuspectedFraudPairRDD.groupByKey().map(lambda x : (x[0], list(x[1])))
	customersPairRDD = customersRDD.map(lambda k: (k[0], (k[1], k[2], k[4], k[5], k[6])))
	customerSuspectedFraud = ordersSuspectedFraudPairRDDGrouped.join(customersPairRDD)
	customerSuspectedFraud.coalesce(1).saveAsTextFile("/home/hduser/Downloads/analysis_on_retail_data/suspectedFrauds")

	#3 to find total amount and total number of orders for each of value of paymentStatus
	orderStatusRDD = orders.map(lambda k: (k.split(",")[3], 1))
	orderStatusCount = orderStatusRDD.reduceByKey(lambda x,y: (x+y))
	orderStatusCount.coalesce(1).saveAsTextFile("/home/hduser/Downloads/analysis_on_retail_data/orderAmount")
	orderItems = sc.textFile("/home/hduser/Downloads/analysis_on_retail_data/order_items")
	ordersPairRDD = orders.map(lambda k: (k.split(",")[0], k.split(",")[3]))
	orderItemsPairRDD = orderItems.map(lambda k: (k.split(",")[1], k.split(",")[4]))
	joinedRDD = orderItemsPairRDD.join(ordersPairRDD)
	joinedRDDGrouped = joinedRDD.groupByKey().map(lambda k: (k[0], list(k[1])))
	orderPriceAndStatus = [i[1] for i in joinedRDDGrouped.collect()]
	orderPriceAndStatusRDD = sc.parallelize(orderPriceAndStatus)
	orderStatusAndPriceRDD = orderPriceAndStatusRDD.flatMap(lambda xs: [(x[1], x[0]) for x in xs])
	# orderStatusAndPriceRDD.top(2)
	orderStatusAndPrice = orderStatusAndPriceRDD.reduceByKey(lambda x,y: (float(x)+float(y)))
	orderStatusAndPrice.coalesce(1).saveAsTextFile("/home/hduser/Downloads/analysis_on_retail_data/paymentStatus")


	

	customersRDD.unpersist()

	
	
	
