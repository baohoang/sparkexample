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
		List<Tuple2<Long, Long>> a1=new ArrayList<Tuple2<Long, Long>>();
		a1.add(new Tuple2<Long, Long>(1L,2L));
		a1.add(new Tuple2<Long, Long>(2L,3L));
		a1.add(new Tuple2<Long, Long>(1L,2L));
		SparkConf conf=new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext();
		JavaPairRDD<Long,Long> c=sc.parallelizePairs(a1);
		a1=c.distinct().collect();
		for(Tuple2<Long, Long> s:a1){
			System.out.println(s._1()+" "+s._2());
		}
		sc.stop();
	}
}
