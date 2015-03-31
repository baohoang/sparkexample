package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataType;

import scala.Tuple2;
import scala.collection.Seq;
import vn.wss.spark.model.RModel;
import vn.wss.spark.model.SimilarModel;

public class PreRecommendation {
	private static final Logger logger = LogManager
			.getLogger(PreRecommendation.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame visitorsFrame = sqlContext.load("/spark/visitors/parquet");
		DataFrame similarsFrame = sqlContext.load("/spark/similars/parquet");
		JavaPairRDD<Long, RModel> addC = similarsFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, RModel>() {

					@Override
					public Tuple2<Long, RModel> call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						logger.info(v1.toString());
						RModel model = new RModel(v1.getLong(0), v1.getLong(1),
								-1, -1, v1.getInt(2));
						return new Tuple2<Long, RModel>(v1.getLong(0), model);
					}
				});
		JavaPairRDD<Long, RModel> addA = visitorsFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, RModel>() {

					@Override
					public Tuple2<Long, RModel> call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						logger.info(v1.toString());
						RModel model = new RModel(v1.getLong(0), -1L, v1
								.getInt(1), -1, -1);
						return new Tuple2<Long, RModel>(v1.getLong(0), model);
					}
				});
		JavaPairRDD<Long, RModel> addB = visitorsFrame.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, RModel>() {

					@Override
					public Tuple2<Long, RModel> call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						logger.info(v1.toString());
						RModel model = new RModel(-1L, v1.getLong(0), -1, v1
								.getInt(1), -1);
						return new Tuple2<Long, RModel>(v1.getLong(0), model);
					}
				});
		JavaRDD<RModel> rate = addC
				.union(addA)
				.reduceByKey(new Function2<RModel, RModel, RModel>() {

					@Override
					public RModel call(RModel v1, RModel v2) throws Exception {
						// TODO Auto-generated method stub
						long itemId = Math.max(v1.getItemId(), v2.getItemId());
						long similarId = Math.max(v1.getSimilarId(),
								v2.getSimilarId());
						int a = Math.max(v1.getA(), v2.getA());
						int b = Math.max(v1.getB(), v2.getB());
						int c = Math.max(v1.getC(), v2.getC());
						return new RModel(itemId, similarId, a, b, c);
					}
				})
				.mapToPair(
						new PairFunction<Tuple2<Long, RModel>, Long, RModel>() {

							@Override
							public Tuple2<Long, RModel> call(
									Tuple2<Long, RModel> t) throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<Long, RModel>(t._2()
										.getSimilarId(), t._2());
							}
						}).union(addB)
				.reduceByKey(new Function2<RModel, RModel, RModel>() {

					@Override
					public RModel call(RModel v1, RModel v2) throws Exception {
						// TODO Auto-generated method stub
						long itemId = Math.max(v1.getItemId(), v2.getItemId());
						long similarId = Math.max(v1.getSimilarId(),
								v2.getSimilarId());
						int a = Math.max(v1.getA(), v2.getA());
						int b = Math.max(v1.getB(), v2.getB());
						int c = Math.max(v1.getC(), v2.getC());
						return new RModel(itemId, similarId, a, b, c);
					}
				}).values();
		DataFrame res = sqlContext.createDataFrame(rate, RModel.class);
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/ratings/parquet"))) {
			hdfs.delete(new Path("/spark/ratings/parquet"), true);
		}
		res.saveAsParquetFile("/spark/ratings/parquet");
		sc.stop();
	}
}
