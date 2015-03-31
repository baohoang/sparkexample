package vn.wss.spark.recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import vn.wss.spark.model.Visitors;

public class Calculation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/typeusers/parquet");
		JavaRDD<Visitors> res = rawFrame.toJavaRDD().map(
				new Function<Row, Visitors>() {

					@Override
					public Visitors call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						long id = v1.getLong(0);
						String list = v1.getString(1);
						String[] numOfVisitors = list.split(",");
						return new Visitors(id, numOfVisitors.length);
					}

				});
		DataFrame data = sqlContext.createDataFrame(res, Visitors.class);
		data.saveAsParquetFile("/spark/visitors/parquet");
		sc.stop();
	}
}
