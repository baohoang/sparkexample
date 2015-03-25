package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.datastax.spark.connector.japi.CassandraRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class CassandraConnection {
	
	private static final Logger logger = LogManager.getLogger(CassandraConnection.class);
	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true)
	      .set("spark.cassandra.connection.host", "10.0.0.11");

			JavaSparkContext sc = new JavaSparkContext(conf);
	    // entire table as an RDD
	    // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
	    JavaRDD<String> data = javaFunctions(sc).cassandraTable("tracking" , "tracking").where("year_month = ?", 201501).map(new Function<CassandraRow, String>() {

			@Override
			public String call(CassandraRow v1) throws Exception {
				// TODO Auto-generated method stub
				
				return v1.toString();
			}
		});
	    logger.info("completed ..."+ data.count());
	    sc.stop();
	    // print some basic stats
	}
}
