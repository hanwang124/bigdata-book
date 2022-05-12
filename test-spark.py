from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.mllib.recommendation import ALS, Rating
import pandas as pd
import os
# os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
def loadBookNames():
    bookNames = {}
    path = "./data/BX-Books.csv"
    Book = pd.read_csv(path, sep=None, error_bad_lines=False)
    return Book

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = fields[1], rating = int(fields[2]))

if __name__ == "__main__":
    # # The main script - create our SparkContext
    # conf = SparkConf().setMaster("spark://172.29.88.66:7077").setAppName("SparkWriteApplication")
    # sc = SparkContext(conf = conf) 172.29.95.253
    
    spark = SparkSession.\
        builder.\
        appName("pyspark-notebook1").\
        master("spark://localhost:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
    
    # Create a SparkSession
    spark = SparkSession.builder.appName("SparkWriteApplication").getOrCreate()
    print('sssssssssssssssssssssss',spark)
    # numbersRdd = sc.parallelize([1,2,3])
    # print(numbersRdd)
    # numbersRdd.saveAsTextFile("hdfs://8.130.15.118:8020/data/test-numbers-as-text")
    lines = sc.textFile("hdfs://172.29.88.66:8020/data/BX-Book-Ratings.csv")
    print('lines............',lines)
    
    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)
    #print(ratingsRDD.collect())
    
    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()
    print('rating',createDataFrame)
    
    # bookNames = loadBookNames()
    # print(bookNames.head())
    
    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="User-ID", itemCol="ISBN", ratingCol="Book-Rating")
    model = als.fit(ratingsRDD)
    
    
    
    spark.stop()
    # ratingResourcesPath = 'hdfs://8.130.15.118:8020/data/BX-Book-Ratings.csv'
    # ratingSamples = sc.read.format('csv').option('header', 'true').load(ratingResourcesPath) \
    #     .withColumn("userIdInt", F.col("User-ID").cast(IntegerType())) \
    #     .withColumn("movieIdInt", F.col("ISBN").cast(IntegerType())) \
    #     .withColumn("ratingFloat", F.col("Book-Rating").cast(FloatType()))

    # # Load up our movie ID -> movie name lookup table
    # movieNames = loadMovieNames()

    # Load up the raw u.data file
    # lines = sc.textFile("12abd6f33558:50075/webhdfs/v1/data/BX-Book-Ratings.csv")
    
    
    # print(lines)
    # # Convert to (movieID, (rating, 1.0))
    # movieRatings = lines.map(parseInput)

    # # Reduce to (movieID, (sumOfRatings, totalRatings))
    # ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # # Map to (movieID, averageRating)
    # averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # # Sort by average rating
    # sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # # Take the top 10 results
    # results = sortedMovies.take(10)

    # # Print them out:
    # for result in results:
    #     print(movieNames[result[0]], result[1])