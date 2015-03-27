package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
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
		Map<String, Integer> map = javaFunctions(sc)
				.cassandraTable("tracking", "tracking")
				.select("year_month", "at", "uri")
				.where("year_month = ?", 201502)
				.map(new Function<CassandraRow, String>() {

					@Override
					public String call(CassandraRow v1) throws Exception {
						// TODO Auto-generated method stub
						logger.info(v1.getString("uri"));
						String uri = v1.getString("uri");
						if (uri != null) {
							return v1.getString("uri");
						}
						return "";
					}
				}).filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.startsWith("http://websosanh.vn/s/");
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(StringUtils
								.getWordSearch(t), 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}).collectAsMap();
		String path = "/home/hdspark/wordcount";
		File f = new File(path, "word.txt");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			for (Entry<String, Integer> e : map.entrySet()) {
				bw.write(e.getKey() + ", " + e.getValue());
			}
			bw.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		sc.stop();
	}
}
