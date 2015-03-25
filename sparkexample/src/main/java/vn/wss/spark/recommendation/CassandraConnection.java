package vn.wss.spark.recommendation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import com.datastax.spark.connector.japi.CassandraRow;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class CassandraConnection {
	
	private static final Logger logger = LogManager.getLogger(CassandraConnection.class);
	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true)
	      .set("spark.cassandra.connection.host", "10.0.0.11");

			JavaSparkContext sc = new JavaSparkContext(conf);
	    // entire table as an RDD
	    // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
	    JavaRDD<CassandraRow> data = javaFunctions(sc).cassandraTable("tracking" , "tracking");
	    
	    logger.info("completed ..."+ data.count());
	    sc.stop();
	    // print some basic stats
	}
}
