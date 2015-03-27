package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
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
//		File f = new File("/home/hdspark/wordcount/word.txt");
//		BufferedReader brItem = new BufferedReader(new FileReader(f));
//		String s = null;
//		
//		Map<Long, String> listItems = new HashMap<Long, String>();
//		while ((s = brItem.readLine()) != null) {
//			String st[] = s.split(" ");
//			long id = Long.valueOf(st[0]);
//			String uri = st[1];
//			listItems.put(id, uri);
//		}
//		brItem.close();
//		return listItems;
	}
}
