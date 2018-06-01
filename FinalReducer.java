package mapredassignment;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * FinalReducer is a reducer class for reducing and combining 
 * the movie and reviews data together.
 * In this NoOf Reviews calculated for each movies.
 * Output is sorted on the basis of popularity/NoOfReviews
 */

public class FinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	Map<String, Integer> movieReviewCountMap = new HashMap<String, Integer>();

	/*
	 * (Set up method is called at the start of Reduce i.e. before the actual Reduce
	 * function to do some tranformation before map)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	/*
	 * Output of Movie Mapper and Review Mapper are merged On basis of the count for
	 * the movie Same Movie Name Handled Putting Movie Name and No Of Review for
	 * each movie Having review atleast one in Map
	 * 
	 */
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int noOfReviews = 0;
		String tmp = "";
		String movieName = "";
		int rep = 0;
		Iterator<Text> iter = values.iterator();

		while (iter.hasNext()) {
			tmp = iter.next().toString();
			String record[] = tmp.split("%");

			if (record[0].equalsIgnoreCase("movies")) {
				movieName = record[1];

			} else if (record[0].equalsIgnoreCase("reviews")) {
				noOfReviews++;
			}
		}

		if (movieReviewCountMap.containsKey(movieName)) {
			movieName = movieName + "_" + rep++;
		}

		if (noOfReviews > 0) {
			movieReviewCountMap.put(movieName, noOfReviews);
		}
	}

	/*
	 * Method for returning the sorted map. Comparator is used to compare the value
	 * of the map sort the output according to value and put it in map And return it.
	 */
	public static Map<String, Integer> returnSortedMap(Map<String, Integer> input) {
		LinkedList<Map.Entry<String, Integer>> entries = new LinkedList<Map.Entry<String, Integer>>(input.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o2.getValue().compareTo(o1.getValue()));
			}
		});

		Map<String, Integer> returnMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : entries) {
			returnMap.put(entry.getKey(), entry.getValue());
		}
		return returnMap;
	}

	/*
	 * (Cleanup is called once at the end to finish off anything for reducer Here
	 * final out is written to file in HDFS)
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, Integer> sortedMovieReviewCountMap = new HashMap<String, Integer>();
		sortedMovieReviewCountMap = returnSortedMap(movieReviewCountMap);

		for (Map.Entry<String, Integer> entry : sortedMovieReviewCountMap.entrySet()) {
			context.write(new IntWritable(entry.getValue()), new Text(entry.getKey()));
		}
	}

}
