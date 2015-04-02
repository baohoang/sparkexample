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

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		List<PModel> a = new ArrayList<PModel>();
		a.add(new PModel(0, 1));
		a.add(new PModel(0, 2));
		a.add(new PModel(0, 3));
		List<Tuple2<Integer, Integer>> b = new ArrayList<Tuple2<Integer, Integer>>();
		b.add(new Tuple2<Integer, Integer>(0, 1));
		b.add(new Tuple2<Integer, Integer>(1, 1));
		JavaRDD<PModel> x1 = sc.parallelize(a);
		JavaPairRDD<Integer, Integer> x2 = sc.parallelizePairs(b);
		DataFrame dataFrame=sqlContext.createDataFrame(x1, PModel.class);
		dataFrame.registerTempTable("pmodel");
		List<PModel> a1= new ArrayList<PModel>();
		a1.add(new PModel(1, 1));
		a1.add(new PModel(1, 2));
		a.add(new PModel(0, 4));
		JavaRDD<PModel> x3 = sc.parallelize(a1);
		DataFrame dataFrame1=sqlContext.createDataFrame(x3, PModel.class);
//		dataFrame1.insertInto("pmodel", true);
		DataFrame d=dataFrame.filter("itemID=3");
		d.insertInto("pmodel");
		d=sqlContext.table("pmodel");
		d.show();
		sc.stop();
	}

}
