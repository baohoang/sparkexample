package vn.wss.spark.recommendation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.joda.time.DateTime;

import scala.Tuple2;
import vn.wss.spark.model.ArrayLongListWritable;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.RModel;

public class SparkSQLExample {
	private static final String FILE_PATH = "/spark";
	private static final String USER_ITEM = FILE_PATH + "/user4item";
	private static final String RESULT_PATH = FILE_PATH + "/result";

	private static final Logger logger = LogManager
			.getLogger(SparkSQLExample.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame data = sqlContext.load("/spark/rawdata/parquet");
		//get from raw data 
		JavaPairRDD<Long, Long> rawData = data.toJavaRDD().mapToPair(
				new PairFunction<Row, Long, Long>() {

					@Override
					public Tuple2<Long, Long> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						long key = t.getLong(0);
						long val = t.getLong(1);
						return new Tuple2<Long, Long>(key, val);
					}
				});
		JavaPairRDD<Long, Long> a = RecommendationSpark.calculate(rawData);
		JavaPairRDD<Tuple2<Long, Long>, Long> c = RecommendationSpark
				.calculateSimilar(rawData);
		JavaRDD<RModel> res = RecommendationSpark.fusion(c, a);
		DataFrame schema = sqlContext.createDataFrame(res, RModel.class);
		schema.saveAsParquetFile("/spark/recommendation/rating/parquet");

		// data.save("/spark/rawdata/parquet", SaveMode.Overwrite);
		// data.registerTempTable("raw_data");
		sc.stop();
	}

}
