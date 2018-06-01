# Hadoop BigData MapReduce-Hive-MovieRanking
MapReduce &amp; Hive Query on the Movies &amp; Rating dataset to predict movie on the basis of popularity(rating) .


There are two csv files in each of the dataset: reviews.csv, movies.csv. The schema is shown as follow:
Reviews(userId, movieId, rating, timestamp)
Movies(movieId, title, genres)

Implementing  queries on both MapReduce and Hive. It is recommended that you solve the problem on Hive first so that you understand what the result should looks like. Then, develop your own MapReduce code and compare your result with the previous result.

-Ranking the movie by their number of reviews(popularity). sorting the result in either ascending or descending order.
