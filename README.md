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








# EXPERIMENT 9: Get same transactions from both months

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# Load both month files
month1 = sc.textFile("month1.csv")
month2 = sc.textFile("month2.csv")

# Remove header
header1 = month1.first()
header2 = month2.first()

month1 = month1.filter(lambda x: x != header1)
month2 = month2.filter(lambda x: x != header2)

# Create pair RDD: (Account, (Source, Amount))
rdd1 = month1.map(lambda x: x.split(",")) \
             .map(lambda x: (x[0], (x[1], float(x[2]))))

rdd2 = month2.map(lambda x: x.split(",")) \
             .map(lambda x: (x[0], (x[1], float(x[2]))))

# Join both RDDs on Account
joined_rdd = rdd1.join(rdd2)

# Filter same source and same amount
same_transactions = joined_rdd.filter(
    lambda x: x[1][0][0] == x[1][1][0] and
              x[1][0][1] == x[1][1][1]
)

# Print first 10 same transactions
print("First 10 same transactions:")
for record in same_transactions.take(10):
    print(record)

# Count same transactions for each account
print("\nCount of same transactions for each account:")
for account, count in same_transactions.countByKey().items():
    print(account, count)
