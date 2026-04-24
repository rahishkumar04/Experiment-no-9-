# Experiment-no-9-from pyspark import SparkContext

# Spark initialize karein
sc = SparkContext.getOrCreate()

# Sample Data (Aap isse change bhi kar sakte hain)
m1 = [(101, ("ATM", 500)), (102, ("Online", 1000)), (103, ("UPI", 200))]
m2 = [(101, ("ATM", 500)), (102, ("UPI", 1000)), (103, ("UPI", 200))]

rdd1 = sc.parallelize(m1)
rdd2 = sc.parallelize(m2)

# JOIN logic
joined = rdd1.join(rdd2)

# FILTER logic (Source and Amount same check karna)
# x[1][0] is value from month 1, x[1][1] is value from month 2
same_tx = joined.filter(lambda x: x[1][0] == x[1][1])

# OUTPUT
print("Matching Transactions (Top 10):")
for r in same_tx.take(10):
    print(r)

# COUNT by key
counts = same_tx.countByKey()
print("\nCounts per Account:", dict(counts))
