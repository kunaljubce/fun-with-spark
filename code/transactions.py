from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
	conf = SparkConf().setAppName("transactions").setMaster("local[*]")
	sc = SparkContext(conf = conf)

	trans08 = sc.textFile("/home/hduser/Downloads/Non_graded_Case_Study/transactions_08.csv")
	trans09 = sc.textFile("/home/hduser/Downloads/Non_graded_Case_Study/transactions_09.csv")

	trans08RDD = trans08.map(lambda line: (line.split(",")[1], line.split(",")[2], line.split(",")[3]))
	trans09RDD = trans09.map(lambda line: (line.split(",")[1], line.split(",")[2], line.split(",")[3]))
	# grouping the RDDs based on all 3 elements and collecting the values
	trans08RDDGrouped = trans08RDD.groupBy(lambda k: (k[0],k[1],k[2])).map(lambda x : (x[0], list(x[1]))).collect()
	trans09RDDGrouped = trans09RDD.groupBy(lambda k: (k[0],k[1],k[2])).map(lambda x : (x[0], list(x[1]))).collect()
	# converting the lists, returned after groupBy, to RDDs again
	trans08RDDGroupedRDD = sc.parallelize(trans08RDDGrouped)
	trans09RDDGroupedRDD = sc.parallelize(trans09RDDGrouped)

	transCombined = trans08RDDGroupedRDD.cogroup(trans09RDDGroupedRDD)
	# print(type(transCombined))
	transCombinedFiltered = transCombined.filter(lambda x: ((list(x[0]), list(x[1])) if len(list(x[1][1])) > 0 and len(list(x[1][0])) > 0 else ""))
																																																																																																																																																																															  
	transCombinedFiltered.mapValues(lambda x: (list(x[0]), list(x[1]))).coalesce(1).saveAsTextFile("/home/hduser/Desktop/spark/matchingTransactions")

	# To find the accounts with transactions in only 1 month
	trans08RDDGroupedAcOnly = trans08RDD.groupBy(lambda k: k[0]).map(lambda x : (x[0], list(x[1]))).collect()
	trans09RDDGroupedAcOnly = trans09RDD.groupBy(lambda k: k[0]).map(lambda x : (x[0], list(x[1]))).collect()
	trans08RDDGroupedAcOnlyRDD = sc.parallelize(trans08RDDGroupedAcOnly)
	trans09RDDGroupedAcOnlyRDD = sc.parallelize(trans09RDDGroupedAcOnly)
	trans08Unique = trans08RDDGroupedAcOnlyRDD.subtractByKey(trans09RDDGroupedAcOnlyRDD)
	trans09Unique = trans09RDDGroupedAcOnlyRDD.subtractByKey(trans08RDDGroupedAcOnlyRDD)
	transUniqueTotal = trans08Unique.union(trans09Unique)
	transUniqueTotal.mapValues(lambda x: (list(x[0]))).coalesce(1).saveAsTextFile("/home/hduser/Desktop/spark/nonMatchingTransactions")	


