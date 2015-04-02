package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.PModel;

public class SparkSQLExample {
	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		List<PModel> a = new ArrayList<PModel>();
		a.add(new PModel(0, 1));
		a.add(new PModel(0, 2));
		a.add(new PModel(0, 3));
		List<PModel> g = new ArrayList<PModel>();
		g.add(new PModel(0, 1));
		g.add(new PModel(0, 2));
		g.add(new PModel(0, 3));
		List<Tuple2<Integer, Integer>> b = new ArrayList<Tuple2<Integer, Integer>>();
		b.add(new Tuple2<Integer, Integer>(0, 1));
		b.add(new Tuple2<Integer, Integer>(1, 1));
		b.add(new Tuple2<Integer, Integer>(3, 4));
		List<Tuple2<Integer, Integer>> c = new ArrayList<Tuple2<Integer, Integer>>();
		c.add(new Tuple2<Integer, Integer>(0, 1));
		c.add(new Tuple2<Integer, Integer>(1, 1));
		c.add(new Tuple2<Integer, Integer>(3, 4));
		c.add(new Tuple2<Integer, Integer>(3, 5));
		JavaRDD<PModel> x1 = sc.parallelize(a);
		JavaRDD<PModel> x0 = sc.parallelize(g);
		DataFrame d1=sqlContext.createDataFrame(x1, PModel.class);
		DataFrame d2=sqlContext.createDataFrame(x0, PModel.class);
		DataFrame dataFrame=d1.unionAll(d2);
		dataFrame.show();
//		JavaPairRDD<Integer, Integer> x2 = sc.parallelizePairs(b);
//		JavaPairRDD<Integer, Integer> x3 = sc.parallelizePairs(c);
//		JavaPairRDD<Integer, Integer> x4 = x2.union(x3);
//		logger.info(x2.collectAsMap().toString());
//		logger.info(x4.collectAsMap().toString());
		// JavaRDD<PModel> x3 = sc.parallelize(a1);
		// DataFrame dataFrame1=sqlContext.createDataFrame(x3, PModel.class);
		// // dataFrame1.insertInto("pmodel", true);
		// DataFrame d=dataFrame.filter("itemID=3");
		// d.collectAsList();
		// d.insertInto("pmodel");
		// sqlContext.sql("INSERT INTO pmodel VALUES (3,0)");
		// d=sqlContext.table("pmodel");
		// d.show();
		// DataFrame rawFrame = sqlContext.load("/spark/rawdata/parquet");
		// rawFrame.printSchema();
		// DataFrame similarFrame = sqlContext.load("/spark/similars/parquet");
		// similarFrame.printSchema();
		// //similarFrame.registerTempTable("similar");
		// DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		// visitorsFrame.printSchema();
		// //visitorsFrame.registerTempTable("visitor");
		// DataFrame itemsFrame = sqlContext.load("/spark/typeitems/parquet");
		// itemsFrame.printSchema();
		// //itemsFrame.registerTempTable("items");
		// DataFrame usersFrame = sqlContext.load("/spark/typeusers/parquet");
		// usersFrame.printSchema();
		// //usersFrame.registerTempTable("users");
		// DataFrame ratingsFrame = sqlContext.load("/spark/ratings/parquet");
		// ratingsFrame.printSchema();
		// //ratingsFrame.registerTempTable("ratings");
		// DataFrame resultFrame = sqlContext.load("/spark/result/parquet");
		// resultFrame.printSchema();
		// Thread thread1 = new Thread(new Runnable() {
		//
		// @Override
		// public synchronized void run() {
		// // TODO Auto-generated method stub
		// logger.info("thread1");
		// }
		// });
		// Thread thread2 = new Thread(new Runnable() {
		//
		// @Override
		// public synchronized void run() {
		// // TODO Auto-generated method stub
		// logger.info("thread2");
		// }
		// });
		// thread1.start();
		// thread2.start();
		sc.stop();
	}

}
