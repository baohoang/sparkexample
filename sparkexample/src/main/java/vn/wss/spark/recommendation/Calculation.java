package vn.wss.spark.recommendation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import vn.wss.spark.model.VModel;

public class Calculation {
	private static final Logger logger = LogManager
			.getLogger(Calculation.class);

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf(true);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame rawFrame = sqlContext.load("/spark/typeitems/parquet");
		JavaRDD<VModel> res = rawFrame.toJavaRDD().map(
				new Function<Row, VModel>() {

					@Override
					public VModel call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						long id = v1.getLong(0);
						String list = v1.getString(1);
						String[] numOfVisitors = list.split(",");
						return new VModel(id, numOfVisitors.length);
					}
				});
		DataFrame data = sqlContext.createDataFrame(res, VModel.class);
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"),
				configuration);
		if (hdfs.exists(new Path("/spark/visitors/parquet"))) {
			hdfs.delete(new Path("/spark/visitors/parquet"), true);
		}
		data.saveAsParquetFile("/spark/visitors/parquet");
		sc.stop();
	}
}
