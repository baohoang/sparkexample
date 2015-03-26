package vn.wss.wordsearch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = javaFunctions(sc)
				.cassandraTable("tracking", "tracking").select("uri")
				.map(new Function<CassandraRow, String>() {

					@Override
					public String call(CassandraRow v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.getString("uri");
					}
				});
		JavaPairRDD<String, Integer> dataMap = data
				.mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String v1)
							throws Exception {
						// TODO Auto-generated method stub
						String uri = v1;
						if (uri != null) {
							String wordSearch = StringUtils.getWordSearch(uri);
							if (wordSearch != null) {
								return new Tuple2<String, Integer>(wordSearch,
										1);
							}
						}
						return null;
					}
				});
		Map<String, Integer> map = dataMap.reduceByKey(
				new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}).collectAsMap();
		String path = "/home/hdspark/wordsearch";

		File f = new File(path, "wordsearchresult.txt");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			for (Entry<String, Integer> e : map.entrySet()) {
				bw.write(e.getKey() + " " + e.getValue());
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sc.stop();
	}
}
