package vn.wss.spark.recommendation;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;

import scala.Tuple2;
import scala.reflect.internal.Trees.New;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.RModel;
import vn.wss.spark.model.SModel;
import vn.wss.spark.model.TModel;

public class UpdateC {

	private DataFrame inputFrame;
	private DataFrame similarsFrame;
	private DataFrame usersFrame;

	public void run() {
		// TODO Auto-generated method stub
		JavaPairRDD<Long, String> input = inputFrame.toJavaRDD()
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
		JavaPairRDD<Long, String> users = usersFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, String>() {

					@Override
					public Tuple2<Long, String> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long key = t.getLong(0);
						String val = t.getString(1);
						return new Tuple2<Long, String>(key, val);
					}
				});
		JavaRDD<TModel> usersNew = users
				.fullOuterJoin(input)
				.map(new Function<Tuple2<Long, Tuple2<Optional<String>, Optional<String>>>, TModel>() {

					@Override
					public TModel call(
							Tuple2<Long, Tuple2<Optional<String>, Optional<String>>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						Optional<String> op1 = v1._2()._1();
						Optional<String> op2 = v1._2()._2();
						String s = "";
						if (op1.isPresent()) {
							s += op1.get();
						}
						if (op2.isPresent()) {
							s += op2.get();
						}
						return new TModel(v1._1(), s);
					}
				});
		// save users list

		// calc number of similar new
		JavaPairRDD<Tuple2<Long, Long>, Integer> addSimilar = users
				.join(input)
				.flatMapToPair(
						new PairFlatMapFunction<Tuple2<Long, Tuple2<String, String>>, Tuple2<Long, Long>, Integer>() {

							@Override
							public Iterable<Tuple2<Tuple2<Long, Long>, Integer>> call(
									Tuple2<Long, Tuple2<String, String>> t)
									throws Exception {
								// TODO Auto-generated method stub
								List<Tuple2<Tuple2<Long, Long>, Integer>> res = new ArrayList<Tuple2<Tuple2<Long, Long>, Integer>>();
								String[] optional = t._2()._2().split(",");
								String[] list = t._2()._1().split(",");
								for (int i = 0; i < optional.length; i++) {
									long a = Long.parseLong(optional[i]);
									for (int j = i + 1; j < optional.length; j++) {
										long b = Long.parseLong(optional[j]);
										if (a < b) {
											res.add(new Tuple2<Tuple2<Long, Long>, Integer>(
													new Tuple2<>(a, b), 1));
										} else {
											if (a > b) {
												res.add(new Tuple2<Tuple2<Long, Long>, Integer>(
														new Tuple2<>(b, a), 1));
											}
										}
									}
								}
								for (int i = 0; i < optional.length; i++) {
									long a = Long.parseLong(optional[i]);
									for (int j = 0; j < list.length; j++) {
										long b = Long.parseLong(list[j]);
										if (a < b) {
											res.add(new Tuple2<Tuple2<Long, Long>, Integer>(
													new Tuple2<>(a, b), 1));
										} else {
											if (a > b) {
												res.add(new Tuple2<Tuple2<Long, Long>, Integer>(
														new Tuple2<>(b, a), 1));
											}
										}
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
		JavaPairRDD<Tuple2<Long, Long>, Integer> similars = similarsFrame
				.toJavaRDD().mapToPair(
						new PairFunction<Row, Tuple2<Long, Long>, Integer>() {

							@Override
							public Tuple2<Tuple2<Long, Long>, Integer> call(
									Row t) throws Exception {
								// TODO Auto-generated method stub
								long id1 = t.getLong(0);
								long id2 = t.getLong(1);
								int numOfS = t.getInt(2);
								return new Tuple2<Tuple2<Long, Long>, Integer>(
										new Tuple2<>(id1, id2), numOfS);
							}
						});
		JavaRDD<SModel> similarsNew=similars.fullOuterJoin(addSimilar)
				.map(new Function<Tuple2<Tuple2<Long, Long>, Tuple2<Optional<Integer>, Optional<Integer>>>, SModel>() {

					@Override
					public SModel call(
							Tuple2<Tuple2<Long, Long>, Tuple2<Optional<Integer>, Optional<Integer>>> v1)
							throws Exception {
						// TODO Auto-generated method stub
						int num = 0;
						Optional<Integer> op1 = v1._2()._1();
						Optional<Integer> op2 = v1._2()._2();
						if (op1.isPresent()) {
							num += op1.get();
						}
						if (op2.isPresent()) {
							num += op2.get();
						}
						return new SModel(v1._1()._1(), v1._1()._2(), num);
					}
				});
		//save
	}

}
