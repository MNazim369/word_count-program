# word_count-program
from pyspark.sql  import SparkSession
# Use the existing SparkSession
spark = SparkSession.builder.appName("word count program").getOrCreate()
# Step 1: Load the input text file
text_file = spark.sparkContext.textFile("/FileStore/tables/wordc.txt")
# Step 2: Split the lines into words
words = text_file.flatMap(lambda line:line.split(" "))
# Step 3: Map each word to a (word, 1) pair
word_pair = words.map(lambda w: (w, 1))
# Step 4: Reduce by key to count occurrences of each word
word_counts = word_pair.reduceByKey(lambda a,b:a + b)
# Step 5: Collect and display the results
for word , count in word_counts.collect():
    print(f"{word}: {count}")
