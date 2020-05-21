def recordsPerPartition(df_or_rdd):
	'''
	Function to get records per partition of an RDD or a DF.
	'''
	def countInPartitions(iterator):
		yield sum(1 for _ in iterator)
	try:
		results = df_or_rdd.mapPartitions(countInPartitions).collect()
	except AttributeError:
		results = df_or_rdd.rdd.mapPartitions(countInPartitions).collect()
	for res in results:
		print("* ",res )