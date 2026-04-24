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







experiment no 5


{
 "nbformat": 4,
 "nbformat_minor": 5,
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python",
   "version": "3.10.0"
  },
  "colab": {
   "provenance": []
  }
 },
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Practical 4 & 5 — Bank Transaction Analysis using Apache Spark RDD"
   ],
   "id": "intro-markdown"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "!pip install pyspark"
   ],
   "id": "install-pyspark"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "transactions_08 = \"\"\"2024-08-01 10:20:00,1001,ATM,5000\n",
    "2024-08-02 12:45:00,1002,UPI,1200\n",
    "2024-08-03 14:30:00,1003,NEFT,7000\n",
    "2024-08-04 09:15:00,1004,ATM,3000\n",
    "2024-08-05 16:10:00,1005,ONLINE,4500\n",
    "2024-08-06 18:20:00,1001,UPI,2000\n",
    "2024-08-07 11:00:00,1002,ATM,1000\n",
    "2024-08-08 10:50:00,1006,ONLINE,8000\n",
    "2024-08-09 13:30:00,1007,NEFT,6500\n",
    "2024-08-10 17:40:00,1008,ATM,2200\"\"\"\n",
    "\n",
    "transactions_09 = \"\"\"2024-09-01 09:10:00,1001,ATM,5000\n",
    "2024-09-02 11:20:00,1002,UPI,1200\n",
    "2024-09-03 15:00:00,1003,NEFT,7000\n",
    "2024-09-04 12:30:00,1004,ATM,3500\n",
    "2024-09-05 14:40:00,1005,ONLINE,4500\n",
    "2024-09-06 10:15:00,1011,ATM,900\n",
    "2024-09-07 16:25:00,1012,UPI,3000\n",
    "2024-09-08 18:00:00,1006,ONLINE,8000\n",
    "2024-09-09 19:10:00,1013,NEFT,5000\n",
    "2024-09-10 20:30:00,1014,ATM,1200\"\"\"\n",
    "\n",
    "with open(\"transactions_08.csv\", \"w\") as f:\n",
    "    f.write(transactions_08)\n",
    "\n",
    "with open(\"transactions_09.csv\", \"w\") as f:\n",
    "    f.write(transactions_09)\n",
    "\n",
    "print(\"Files created successfully!\")"
   ],
   "id": "create-csv"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark import SparkContext\n",
    "\n",
    "sc = SparkContext(\"local\", \"BankTransactionAnalysis\")\n",
    "\n",
    "transactions08 = sc.textFile(\"transactions_08.csv\")\n",
    "transactions09 = sc.textFile(\"transactions_09.csv\")\n",
    "\n",
    "print(\"August transactions (first 3):\")\n",
    "for line in transactions08.take(3):\n",
    "    print(line)\n",
    "\n",
    "print(\"\\nSeptember transactions (first 3):\")\n",
    "for line in transactions09.take(3):\n",
    "    print(line)"
   ],
   "id": "start-spark"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def parse(line):\n",
    "    fields = line.split(\",\")\n",
    "    account = fields[1]\n",
    "    source  = fields[2]\n",
    "    amount  = int(fields[3])\n",
    "    return (account, (source, amount))\n",
    "\n",
    "rdd08 = transactions08.map(parse)\n",
    "rdd09 = transactions09.map(parse)\n",
    "\n",
    "print(\"August Pair RDD:\")\n",
    "for x in rdd08.take(5):\n",
    "    print(x)\n",
    "\n",
    "print(\"\\nSeptember Pair RDD:\")\n",
    "for x in rdd09.take(5):\n",
    "    print(x)"
   ],
   "id": "create-pair-rdd"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "common_transactions = rdd08.intersection(rdd09)\n",
    "\n",
    "print(\"Transactions common in BOTH months:\")\n",
    "for x in common_transactions.collect():\n",
    "    print(x)"
   ],
   "id": "common-transactions"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "accounts08 = rdd08.keys()\n",
    "accounts09 = rdd09.keys()\n",
    "\n",
    "only_in_aug = accounts08.subtract(accounts09).distinct()\n",
    "only_in_sep = accounts09.subtract(accounts08).distinct()\n",
    "\n",
    "print(\"Accounts ONLY in August:\")\n",
    "print(only_in_aug.collect())\n",
    "\n",
    "print(\"\\nAccounts ONLY in September:\")\n",
    "print(only_in_sep.collect())"
   ],
   "id": "one-month-only"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "one_month_accounts = only_in_aug.union(only_in_sep).distinct()\n",
    "one_month_accounts.saveAsTextFile(\"one_month_only_accounts\")\n",
    "\n",
    "print(\"Saved! Accounts in only one month:\")\n",
    "print(one_month_accounts.collect())"
   ],
   "id": "save-output"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "sc.stop()\n",
    "print(\"Spark stopped.\")"
   ],
   "id": "stop-spark"
  }
 ]
}
