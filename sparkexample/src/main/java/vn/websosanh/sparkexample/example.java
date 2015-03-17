package vn.websosanh.sparkexample;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

public class example {
	private static final Logger logger = LogManager.getLogger(example.class);
	public static void main(String args[]){
		  final Pattern SPACE = Pattern.compile(" ");
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    JavaRDD<String> lines = ctx.textFile(args[0], 1);
	    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 3445364176768181291L;

			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(SPACE.split(arg0));
			}
	    });
	    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 2727010681430223565L;

			public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	      });
	    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 8714901809537454488L;

			public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });
		List<Tuple2<String, Integer>> output = counts.collect();
	    for (Tuple2<?,?> tuple : output) {
	      logger.info(tuple._1() + ": " + tuple._2());
	    }
	    ctx.stop();
	}
}
