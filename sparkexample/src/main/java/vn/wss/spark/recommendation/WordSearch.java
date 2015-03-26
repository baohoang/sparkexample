package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import vn.wss.util.StringUtils;

import com.datastax.spark.connector.japi.CassandraRow;

public class WordSearch {
	private static final Logger logger = LogManager.getLogger(WordSearch.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, Integer> data = javaFunctions(sc)
				.cassandraTable("tracking", "tracking").select("uri")
				.mapToPair(new PairFunction<CassandraRow, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(CassandraRow t)
							throws Exception {
						// TODO Auto-generated method stub
						String uri = t.getString("uri");
						String regex = ".*\\/s\\/(.*)\\.htm";
						String regex_1 = "cat-[0-9]*\\/(.*)";
						String regex_2 = "(.*)\\/.*";
						Pattern f1 = Pattern.compile(regex);
						Matcher m = f1.matcher(uri);
						if (m.matches()) {
							Pattern filter1 = Pattern.compile(regex_1);
							String s1 = m.group(1);
							Matcher m_f1 = filter1.matcher(s1);
							if (m_f1.matches()) {
								s1 = m_f1.group(1);
							}
							Pattern filter2 = Pattern.compile(regex_2);
							Matcher m_f2 = filter2.matcher(s1);
							while (m_f2.matches()) {
								s1 = m_f2.group(1);
								m_f2 = filter2.matcher(s1);
							}
							return new Tuple2<String, Integer>(
									s1.toLowerCase(), 1);
						}
						return null;
					}
				});
		Map<String, Integer> map = data.reduceByKey(
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
