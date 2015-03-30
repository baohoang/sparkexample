package vn.wss.wordsearch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import tachyon.thrift.WorkerService.Processor.returnSpace;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class WordSearch {
	private static final Logger logger = LogManager.getLogger(WordSearch.class);

	public static void main(String[] args) {
		List<String> a1=new ArrayList<String>();
		a1.add("a");
		a1.add("a");
		a1.add("b");
		List<String> a2=new ArrayList<String>();
		a1.add("c");
		a1.add("b");
		a1.add("b");
		SparkConf conf=new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> c=sc.parallelize(a1);
		JavaRDD<String> c1=sc.parallelize(a2);
		a1=c.union(c1).collect();
		for(String s:a1){
			System.out.println(s);
		}
		sc.stop();
	}
}
