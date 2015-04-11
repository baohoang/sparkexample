package vn.wss.spark.recommendation;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import scala.Tuple2;
import scala.Tuple3;
import vn.wss.spark.model.NewRModel;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.RModel;
import vn.wss.spark.model.Rating;
import vn.wss.spark.model.SModel;
import vn.wss.spark.model.TModel;
import vn.wss.spark.model.TrackingModel;
import vn.wss.spark.model.VModel;
import vn.wss.util.DateUtils;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.base.Optional;

public class Recommendation {
	public static void main(String[] args) throws IOException {
		Date from = DateUtils.readTimeStamp(args[0]);
		Date now = new Date();
		String fromStr = DateUtils.dateToString(from);
		String nowStr = DateUtils.dateToString(now);
		DateUtils.saveTimeStamp(now, args[0]);
		int yearMonth = DateUtils.getYearMonth(now);
		SparkConf conf = new SparkConf(true).set(
				"spark.cassandra.connection.host", "10.0.0.11");

		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraJavaRDD<TrackingModel> rawData = javaFunctions(sc)
				.cassandraTable("tracking", "tracking",
						mapRowTo(TrackingModel.class))
				.select("uri", "user_id")
				.where("year_month = ? AND at > ? and at < ?", yearMonth,
						fromStr, nowStr);
		JavaRDD<TrackingModel> raw = rawData
				.filter(new Function<TrackingModel, Boolean>() {

					@Override
					public Boolean call(TrackingModel v1) throws Exception {
						// TODO Auto-generated method stub
						return v1.getUser_id() != null && v1.getUri() != null
								&& v1.getUri().endsWith("so-sanh.htm");
					}
				});
		JavaRDD<PModel> input = raw.map(new Function<TrackingModel, PModel>() {

			@Override
			public PModel call(TrackingModel v1) throws Exception {
				// TODO Auto-generated method stub
				long userid = Long.parseLong(v1.getUser_id());
				String regex = ".*\\/([0-9]+)\\/so-sanh.htm";
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(v1.getUri());
				String itemIDStr = "-1";
				if (matcher.matches()) {
					itemIDStr = matcher.group(1);
				}
				long itemid = Long.parseLong(itemIDStr);
				return new PModel(userid, itemid);
			}
		}).distinct();// xu li input
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/rawdata/parquet");
		DataFrame similarFrame = sqlContext.load("/spark/similars/parquet");
		DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		DataFrame itemsFrame = sqlContext.load("/spark/typeitems/parquet");
		DataFrame usersFrame = sqlContext.load("/spark/typeusers/parquet");
		DataFrame ratingsFrame = sqlContext.load("/spark/ratings/parquet");
		DataFrame resultFrame = sqlContext.load("/spark/result/parquet");
		DataFrame inputFrame = sqlContext.createDataFrame(input, PModel.class);

		// get subtract
		inputFrame = inputFrame.except(rawFrame);

		// update rawData
		rawFrame = rawFrame.unionAll(inputFrame);

		// new version

		// get resources
		JavaPairRDD<NewRModel, Integer> tb_similars = similarFrame.toJavaRDD()
				.mapToPair(new PairFunction<Row, NewRModel, Integer>() {

					@Override
					public Tuple2<NewRModel, Integer> call(Row t)
							throws Exception {
						// TODO Auto-generated method stub
						NewRModel key = new NewRModel(t.getLong(0), t
								.getLong(1));

						return new Tuple2<NewRModel, Integer>(key, t.getInt(2));
					}
				});
		JavaPairRDD<NewRModel, RModel> r1 = ratingsFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, NewRModel, RModel>() {

					@Override
					public Tuple2<NewRModel, RModel> call(Row t)
							throws Exception {
						// TODO Auto-generated method stub
						long id1 = t.getLong(3);
						long id2 = t.getLong(4);
						int a = t.getInt(0);
						int b = t.getInt(1);
						int c = t.getInt(2);
						return new Tuple2<NewRModel, RModel>(new NewRModel(id1,
								id2), new RModel(id1, id2, a, b, c));
					}
				});
		JavaPairRDD<Long, Integer> tb_visitors = visitorsFrame.toJavaRDD()
				.mapToPair(new PairFunction<Row, Long, Integer>() {

					@Override
					public Tuple2<Long, Integer> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, Integer>(t.getLong(0), t
								.getInt(1));
					}
				});

		JavaPairRDD<Long, String> tb_users = usersFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, String>() {

					@Override
					public Tuple2<Long, String> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long key = t.getLong(0);
						String val = t.getString(1);
						return new Tuple2<Long, String>(key, val);
					}
				});

		// update C
		JavaPairRDD<Long, String> input_v1 = inputFrame.toJavaRDD()
				.mapToPair(new PairFunction<Row, Long, String>() {

					@Override
					public Tuple2<Long, String> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, String>(t.getLong(1), t
								.getLong(0) + ",");
					}
				}).reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String v1, String v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});
		JavaPairRDD<NewRModel, Integer> addSimilar = tb_users
				.join(input_v1)
				.flatMapToPair(
						new PairFlatMapFunction<Tuple2<Long, Tuple2<String, String>>, NewRModel, Integer>() {

							@Override
							public Iterable<Tuple2<NewRModel, Integer>> call(
									Tuple2<Long, Tuple2<String, String>> t)
									throws Exception {
								// TODO Auto-generated method stub
								List<Tuple2<NewRModel, Integer>> res = new ArrayList<>();
								String[] optional = t._2()._2().split(",");
								String[] list = t._2()._1().split(",");
								for (int i = 0; i < optional.length; i++) {
									long a = Long.parseLong(optional[i]);
									for (int j = i + 1; j < optional.length; j++) {
										long b = Long.parseLong(optional[j]);
										res.add(new Tuple2<NewRModel, Integer>(
												new NewRModel(a, b), 1));
										res.add(new Tuple2<NewRModel, Integer>(
												new NewRModel(b, a), 1));
									}
								}
								for (int i = 0; i < optional.length; i++) {
									long a = Long.parseLong(optional[i]);
									for (int j = 0; j < list.length; j++) {
										long b = Long.parseLong(list[j]);
										res.add(new Tuple2<NewRModel, Integer>(
												new NewRModel(a, b), 1));
										res.add(new Tuple2<NewRModel, Integer>(
												new NewRModel(b, a), 1));
									}
								}
								return res;
							}
						})
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});
		tb_similars = tb_similars
				.fullOuterJoin(addSimilar)
				.mapToPair(
						new PairFunction<Tuple2<NewRModel, Tuple2<Optional<Integer>, Optional<Integer>>>, NewRModel, Integer>() {

							@Override
							public Tuple2<NewRModel, Integer> call(
									Tuple2<NewRModel, Tuple2<Optional<Integer>, Optional<Integer>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								Optional<Integer> op1 = t._2()._1();
								Optional<Integer> op2 = t._2()._2();
								int ad = 0;
								if (op1.isPresent()) {
									ad += op1.get();
								}
								if (op2.isPresent()) {
									ad += op2.get();
								}
								return new Tuple2<NewRModel, Integer>(t._1(),
										ad);
							}
						});

		// update A, B
		JavaPairRDD<Long, Integer> addVisitors = input.mapToPair(
				new PairFunction<PModel, Long, Integer>() {

					@Override
					public Tuple2<Long, Integer> call(PModel t)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Long, Integer>(t.getItemID(), 1);
					}

				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		tb_visitors = tb_visitors
				.fullOuterJoin(addVisitors)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<Optional<Integer>, Optional<Integer>>>, Long, Integer>() {

							@Override
							public Tuple2<Long, Integer> call(
									Tuple2<Long, Tuple2<Optional<Integer>, Optional<Integer>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								Optional<Integer> op1 = t._2()._1();
								Optional<Integer> op2 = t._2()._2();
								int ad = 0;
								if (op1.isPresent()) {
									ad += op1.get();
								}
								if (op2.isPresent()) {
									ad += op2.get();
								}
								return new Tuple2<Long, Integer>(t._1(), ad);
							}
						});

		// update Rating
		JavaRDD<RModel> rating = r1
				.fullOuterJoin(addSimilar)
				.mapToPair(
						new PairFunction<Tuple2<NewRModel, Tuple2<Optional<RModel>, Optional<Integer>>>, Long, RModel>() {

							@Override
							public Tuple2<Long, RModel> call(
									Tuple2<NewRModel, Tuple2<Optional<RModel>, Optional<Integer>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								RModel model = null;
								if (t._2()._1().isPresent()) {
									model = t._2()._1().get();
								} else {
									model = new RModel(t._1().getId1(), t._1()
											.getId2(), 0, 0, 0);
								}
								int c = 0;
								if (t._2()._2().isPresent()) {
									c += t._2()._2().get();
								}
								model.setC(c);
								return new Tuple2<Long, RModel>(
										t._1().getId1(), model);
							}
						})
				.fullOuterJoin(addVisitors)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<Optional<RModel>, Optional<Integer>>>, Long, RModel>() {

							@Override
							public Tuple2<Long, RModel> call(
									Tuple2<Long, Tuple2<Optional<RModel>, Optional<Integer>>> t)
									throws Exception {
								// TODO Auto-generated method stub
								RModel model = null;
								if (t._2()._1().isPresent()) {
									model = t._2()._1().get();
								} else {
									model = new RModel(t._1(), -1, 0, 0, 0);
								}
								int a = 0;
								if (t._2()._2().isPresent()) {
									a += t._2()._2().get();
								}
								model.setA(a);
								return new Tuple2<Long, RModel>(model
										.getSimilarId(), model);
							}
						})
				.fullOuterJoin(addVisitors)
				.map(new Function<Tuple2<Long, Tuple2<Optional<RModel>, Optional<Integer>>>, RModel>() {

					@Override
					public RModel call(
							Tuple2<Long, Tuple2<Optional<RModel>, Optional<Integer>>> t)
							throws Exception {
						// TODO Auto-generated method stub
						RModel model = null;
						if (t._2()._1().isPresent()) {
							model = t._2()._1().get();
						} else {
							model = new RModel(t._1(), -1, 0, 0, 0);
						}
						int a = 0;
						if (t._2()._2().isPresent()) {
							a += t._2()._2().get();
						}
						model.setA(a);
						return model;
					}
				});
		ratingsFrame = sqlContext.createDataFrame(rating, PModel.class);
		JavaRDD<Rating> recommended=rating.mapToPair(
				new PairFunction<RModel, Long, Tuple2<Long, Double>>() {

					@Override
					public Tuple2<Long, Tuple2<Long, Double>> call(RModel t)
							throws Exception {
						// TODO Auto-generated method stub
						double rate = t.getC()
								/ (t.getA() + t.getB() - t.getC());
						return new Tuple2<Long, Tuple2<Long, Double>>(t
								.getItemId(), new Tuple2<Long, Double>(t
								.getSimilarId(), rate));
					}
				})
				.groupByKey()
				.flatMap(
						new FlatMapFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Rating>() {

							@Override
							public Iterable<Rating> call(
									Tuple2<Long, Iterable<Tuple2<Long, Double>>> t)
									throws Exception {
								long id1 = t._1();
								// TODO Auto-generated method stub
								List<Tuple2<Long, Double>> list = new ArrayList<>();
								Iterator<Tuple2<Long, Double>> it = t._2()
										.iterator();
								while (it.hasNext()) {
									list.add(it.next());
								}
								Collections.sort(list,
										new Comparator<Tuple2<Long, Double>>() {

											@Override
											public int compare(
													Tuple2<Long, Double> o1,
													Tuple2<Long, Double> o2) {
												// TODO Auto-generated method
												// stub
												return o1._2() == o1._2() ? 0
														: o1._2() < o1._2() ? -1
																: 1;
											}
										});
								List<Rating> res = new ArrayList<>();
								int size = Math.min(list.size(), 10);
								for (int i = 0; i < size; i++) {
									res.add(new Rating(id1, list.get(i)._1(), i));
								}
								return res;
							}
						});
		//save data
		sc.stop();
	}
}