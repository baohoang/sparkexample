package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import com.google.common.collect.Lists;

import scala.Tuple2;
import vn.wss.spark.model.Rating;

public class RatingsCalculation {
	private static final Logger logger = LogManager
			.getLogger(RatingsCalculation.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rate = sqlContext.load("/spark/ratings/parquet");
		JavaRDD<Rating> result = rate
				.toJavaRDD()
				.flatMapToPair(
						new PairFlatMapFunction<Row, Long, Tuple2<Long, Double>>() {

							@Override
							public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(
									Row t) throws Exception {
								// TODO Auto-generated method stub
								int a = t.getInt(0);
								int b = t.getInt(1);
								int c = t.getInt(2);
								long id1 = t.getLong(3);
								long id2 = t.getLong(4);
								double rating = c / (a + b - c);
								List<Tuple2<Long, Tuple2<Long, Double>>> res = new ArrayList<>();
								res.add(new Tuple2<Long, Tuple2<Long, Double>>(
										id1, new Tuple2<Long, Double>(id2,
												rating)));
								res.add(new Tuple2<Long, Tuple2<Long, Double>>(
										id2, new Tuple2<Long, Double>(id1,
												rating)));

								return res;
							}
						})
				.groupByKey()
				.flatMap(
						new FlatMapFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Rating>() {

							@Override
							public Iterable<Rating> call(
									Tuple2<Long, Iterable<Tuple2<Long, Double>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								List<Rating> res = new ArrayList<Rating>();
								List<Tuple2<Long, Double>> list = Lists
										.newArrayList(t._2());
								Collections.sort(list,
										new Comparator<Tuple2<Long, Double>>() {

											@Override
											public int compare(
													Tuple2<Long, Double> o1,
													Tuple2<Long, Double> o2) {
												// TODO Auto-generated method
												// stub
												return o1._2() > o2._2() ? 1
														: o1._2() < o2._2() ? -1
																: 0;
											}
										});
								int size = list.size() >= 10 ? 10 : list.size();
								for (int i = 0; i < size; i++) {
									res.add(new Rating(t._1(),
											list.get(i)._1(), i));
								}
								return res;
							}
						});
		DataFrame data = sqlContext.createDataFrame(result, Rating.class);
		data.saveAsParquetFile("/spark/result/parquet");
		sc.stop();
	}
}
