package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import vn.wss.util.StringUtils;

import com.datastax.spark.connector.japi.CassandraRow;

public class CalcWordSearch {
	private static final Logger logger = LogManager.getLogger(WordSearch.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);

		JavaSparkContext sc = new JavaSparkContext(conf);
		Map<String,Integer> map=sc.textFile("/spark/wordsearch2", 1)
				.filter(new Function<String, Boolean>() {

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
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				}).collectAsMap();
		
		for (Entry<String, Integer> e : map.entrySet()) {
			
		}
		sc.stop();
	}
}
