#Να υλοποιηθεί το Query 4 χρησιμοποιώντας τo SQL API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση.


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q4").getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df.show()

df.createOrReplaceTempView("yellow_tripdata_2024")

result = spark.sql("""
        SELECT VendorID, COUNT(*) AS total_rides
        FROM yellow_tripdata_2024
        WHERE CAST(date_format(tpep_pickup_datetime, 'HH') AS INT) >= 23 AND CAST(date_format(tpep_pickup_datetime, 'MM') AS INT) < 7
        GROUP BY VendorID""")
result.show(30)




### ΕΔΩ ΔΟΚΙΜΑΖΩ ΚΑΙ ΜΕ ΤΟ CSV ΑΡΧΕΙΟ ###
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv", header=True, inferSchema=True)

df.show()

df.createOrReplaceTempView("yellow_tripdata_2024")

result = spark.sql("""
        SELECT VendorID, COUNT(*) AS total_rides
        FROM yellow_tripdata_2024
        WHERE CAST(date_format(tpep_pickup_datetime, 'HH') AS INT) >= 23 AND CAST(date_format(tpep_pickup_datetime, 'MM') AS INT) < 7
        GROUP BY VendorID""")
result.show(30)


#### Παρατήρησα ότι το parquet αρχείο έκανε 3 λεπτά λιγότερο, σε σχέση με το csv αρχείο. Η μετατροπή σε parquet πιστεύων ήταν και είναι κομβική ###
