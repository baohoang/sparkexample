package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import vn.wss.spark.model.NewRModel;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.RModel;

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
		g.add(new PModel(1, 0));
		g.add(new PModel(2, 0));
		g.add(new PModel(3, 0));
		List<Tuple2<PModel, Integer>> b = new ArrayList<Tuple2<PModel, Integer>>();
		b.add(new Tuple2<PModel, Integer>(a.get(0), 1));
		b.add(new Tuple2<PModel, Integer>(a.get(1), 1));
		b.add(new Tuple2<PModel, Integer>(a.get(2), 4));
		b.add(new Tuple2<PModel, Integer>(g.get(0), 1));
		b.add(new Tuple2<PModel, Integer>(g.get(1), 1));
		b.add(new Tuple2<PModel, Integer>(g.get(2), 4));
		List<Tuple2<Integer, Integer>> c = new ArrayList<Tuple2<Integer, Integer>>();
		c.add(new Tuple2<Integer, Integer>(0, 1));
		c.add(new Tuple2<Integer, Integer>(1, 1));
		c.add(new Tuple2<Integer, Integer>(3, 4));
		c.add(new Tuple2<Integer, Integer>(3, 5));
		JavaRDD<PModel> x1 = sc.parallelize(a);
		List<RModel> g1 = new ArrayList<RModel>();
		g1.add(new RModel(1L, 0L, 1, 1, 1));
		g1.add(new RModel(2L, 0L, 1, 1, 1));
		g1.add(new RModel(3L, 0L, 1, 1, 1));
		List<NewRModel> g2 = new ArrayList<NewRModel>();
		g2.add(new NewRModel(1L, 1L));
		g2.add(new NewRModel(2L, 1L));
		g2.add(new NewRModel(3L, 1L));
		JavaRDD<RModel> x01 = sc.parallelize(g1);
		JavaRDD<NewRModel> x00 = sc.parallelize(g2);
		DataFrame d2 = sqlContext.createDataFrame(x00, NewRModel.class);
		DataFrame d1 = sqlContext.createDataFrame(x01, RModel.class);
		d1=d1.unionAll(d2);
		d1.show();
		// JavaRDD<PModel> x0 = sc.parallelize(g);
		// DataFrame d1=sqlContext.createDataFrame(x1, PModel.class);
		// DataFrame d2=sqlContext.createDataFrame(x0, PModel.class);
		// DataFrame dataFrame=d1.unionAll(d2);
		// dataFrame.show();
		// JavaPairRDD<PModel, Integer> x2 = sc.parallelizePairs(b);
		// Map<PModel,Integer> y=x2.reduceByKey(new Function2<Integer, Integer,
		// Integer>() {
		//
		// @Override
		// public Integer call(Integer v1, Integer v2) throws Exception {
		// // TODO Auto-generated method stub
		// return v1+v2;
		// }
		// }).collectAsMap();
		// logger.info(y.toString());
		// JavaPairRDD<Integer, Integer> x3 = sc.parallelizePairs(c);
		//
		sc.stop();
	}

}
