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
		List<Tuple2<Integer, PModel>> a = new ArrayList<Tuple2<Integer, PModel>>();
		a.add(new Tuple2<Integer, PModel>(0, new PModel(0, 1)));
		a.add(new Tuple2<Integer, PModel>(1, new PModel(0, 2)));
		a.add(new Tuple2<Integer, PModel>(2, new PModel(0, 3)));
		List<Tuple2<Integer, Integer>> b = new ArrayList<Tuple2<Integer, Integer>>();
		b.add(new Tuple2<Integer, Integer>(0, 1));
		b.add(new Tuple2<Integer, Integer>(1, 1));
		JavaPairRDD<Integer, PModel> x1 = sc.parallelizePairs(a);
		JavaPairRDD<Integer, Integer> x2 = sc.parallelizePairs(b);
		JavaPairRDD<Integer, Tuple2<PModel, Integer>> x = x1.join(x2);
		long count = x
				.filter(new Function<Tuple2<Integer, Tuple2<PModel, Integer>>, Boolean>() {

					@Override
					public Boolean call(
							Tuple2<Integer, Tuple2<PModel, Integer>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						return v1._2()._2() == 1;
					}
				}).count();
		logger.info(count);
		// DataFrame data = sqlContext.load("/spark/rawdata/parquet");
		// //get from raw data
		// data.toJavaRDD().foreach(new VoidFunction<Row>() {
		//
		// @Override
		// public void call(Row t) throws Exception {
		// // TODO Auto-generated method stub
		// logger.info(t.toString());
		// }
		// });
		sc.stop();
	}

}
