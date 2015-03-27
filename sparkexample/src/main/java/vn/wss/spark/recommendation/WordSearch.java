package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import vn.wss.util.StringUtils;

import com.datastax.spark.connector.japi.CassandraRow;

public class WordSearch {
	private static final Logger logger = LogManager.getLogger(WordSearch.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		javaFunctions(sc).cassandraTable("tracking", "tracking").select("uri").map(new Function<CassandraRow,String>() {

			@Override
			public String call(CassandraRow v1) throws Exception {
				// TODO Auto-generated method stub
				logger.info(v1.toString());
				return v1.toString();
			}
		}).saveAsTextFile("/spark/wordsearch");
//		JavaPairRDD<String, Integer> data = javaFunctions(sc)
//				.cassandraTable("tracking", "tracking").select("uri")
//				.filter(new Function<CassandraRow, Boolean>() {
//
//					@Override
//					public Boolean call(CassandraRow v1) throws Exception {
//						// TODO Auto-generated method stub
//						if (v1.contains("uri")) {
//							String uri = v1.getString("uri");
//							logger.info(uri);
//							if (uri.startsWith("http://websosanh.vn/s/")) {
//								return true;
//							}
//						}
//						return false;
//					}
//				}).mapToPair(new PairFunction<CassandraRow, String, Integer>() {
//
//					@Override
//					public Tuple2<String, Integer> call(CassandraRow t)
//							throws Exception {
//						// TODO Auto-generated method stub
//						String uri = t.getString("uri");
//						return new Tuple2<String, Integer>(StringUtils
//								.getWordSearch(uri), 1);
//					}
//				});
//		JavaPairRDD<String, Integer> map = data
//				.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//					@Override
//					public Integer call(Integer v1, Integer v2)
//							throws Exception {
//						// TODO Auto-generated method stub
//						return v1 + v2;
//					}
//				});
//		logger.info("count :" + map.count());

		// String path = "/home/hdspark/wordsearch";
		//
		// File f = new File(path, "wordsearchresult.txt");
		// try {
		// BufferedWriter bw = new BufferedWriter(new FileWriter(f));
		// for (Tuple2<String, Integer> e : map) {
		// bw.write(e._1() + " " + e._2());
		// }
		// bw.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		sc.stop();
	}
}
