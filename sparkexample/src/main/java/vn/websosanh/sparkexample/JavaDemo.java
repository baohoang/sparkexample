package vn.websosanh.sparkexample;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions.WriterBuilder;
import com.google.common.base.Optional;

public class JavaDemo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4346764041368771081L;
	private transient SparkConf conf;
	private static final Logger logger = LogManager.getLogger(JavaDemo.class);

	private JavaDemo(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		conf.setJars(new String[] {
				"/home/hdspark/spark/lib/spark-cassandra-connector-java_2.10-1.2.0-SNAPSHOT.jar",
				"/home/hdspark/spark/lib/spark-cassandra-connector_2.10-1.2.0-SNAPSHOT.jar" });
		JavaSparkContext sc = new JavaSparkContext(conf);
		logger.info("create");
		createSimpleExample(sc);
//		logger.info("generateData  ...");
//		generateData(sc);
//		logger.info("compute ... ");
//		compute(sc);
//		logger.info("showResult ...");
//		showResults(sc);
//		logger.info("Stopping ..");
		sc.stop();
	}
	
	private void createSimpleExample(JavaSparkContext sc){
		JavaRDD<String> idRdd=javaFunctions(sc).cassandraTable("java_api", "products").map(new Function<CassandraRow, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(CassandraRow v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.toString();
			}
		});
		logger.info(idRdd.toString());
	}

	private void generateData(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		// Prepare the schema
		logger.info("create table ..");
		try (Session session = connector.openSession()) {
			session.execute("DROP KEYSPACE IF EXISTS java_api");
			session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents TEXT)");
			session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
			session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
		}

		// Prepare the products hierarchy
		List<Product> products = Arrays.asList(new Product(0, "All products",
				""), new Product(1, "Product A", "0"), new Product(4,
				"Product A1", "01"), new Product(5, "Product A2", "01"),
				new Product(2, "Product B", "0"), new Product(6, "Product B1",
						"02"), new Product(7, "Product B2", "02"), new Product(
						3, "Product C", "0"),
				new Product(8, "Product C1", "03"), new Product(9,
						"Product C2", "03"));

		JavaRDD<Product> productsRDD = sc.parallelize(products);
		logger.info("parallelize");
		WriterBuilder wb = javaFunctions(productsRDD).writerBuilder("java_api",
				"products", mapToRow(Product.class));
		logger.info("writerBuild");
		wb.saveToCassandra();
		logger.info("save products complete");

		JavaRDD<Sale> salesRDD = productsRDD.filter(
				new Function<Product, Boolean>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					public Boolean call(Product product) throws Exception {
						return product.getParents().length() == 2;
					}
				}).flatMap(new FlatMapFunction<Product, Sale>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Sale> call(Product product) throws Exception {
				Random random = new Random();
				List<Sale> sales = new ArrayList<>(1000);
				for (int i = 0; i < 1000; i++) {
					sales.add(new Sale(UUID.randomUUID(), product.getId(),
							BigDecimal.valueOf(random.nextDouble())));
				}
				return sales;
			}
		});

		javaFunctions(salesRDD).writerBuilder("java_api", "sales",
				mapToRow(Sale.class)).saveToCassandra();
	}

	private void compute(JavaSparkContext sc) {
		JavaPairRDD<Integer, Product> productsRDD = javaFunctions(sc)
				.cassandraTable("java_api", "products", mapRowTo(Product.class))
				.keyBy(new Function<Product, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Product product) throws Exception {
						return product.getId();
					}
				});

		JavaPairRDD<Integer, Sale> salesRDD = javaFunctions(sc).cassandraTable(
				"java_api", "sales", mapRowTo(Sale.class)).keyBy(
				new Function<Sale, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Sale sale) throws Exception {
						return sale.getProduct();
					}
				});

		JavaPairRDD<Integer, Tuple2<Sale, Product>> joinedRDD = salesRDD
				.join(productsRDD);

		JavaPairRDD<Integer, BigDecimal> allSalesRDD = joinedRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Sale, Product>>, Integer, BigDecimal>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<Integer, BigDecimal>> call(
							Tuple2<Integer, Tuple2<Sale, Product>> input)
							throws Exception {
						Tuple2<Sale, Product> saleWithProduct = input._2();
						List<Tuple2<Integer, BigDecimal>> allSales = new ArrayList<>(
								saleWithProduct._2().getParents().length() + 1);
						// allSales.add(new
						// Tuple2<>(saleWithProduct._1().getProduct(),
						// saleWithProduct._1().getPrice()));
						// for (Integer parentProduct :
						// saleWithProduct._2().getParents()) {
						// allSales.add(new Tuple2<>(parentProduct,
						// saleWithProduct._1().getPrice()));
						// }
						return allSales;
					}
				});

		JavaRDD<Summary> summariesRDD = allSalesRDD.reduceByKey(
				new Function2<BigDecimal, BigDecimal, BigDecimal>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					public BigDecimal call(BigDecimal v1, BigDecimal v2)
							throws Exception {
						return v1.add(v2);
					}
				}).map(new Function<Tuple2<Integer, BigDecimal>, Summary>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Summary call(Tuple2<Integer, BigDecimal> input)
					throws Exception {
				return new Summary(input._1(), input._2());
			}
		});

		javaFunctions(summariesRDD).writerBuilder("java_api", "summaries",
				mapToRow(Summary.class)).saveToCassandra();
	}

	private void showResults(JavaSparkContext sc) {
		JavaPairRDD<Integer, Summary> summariesRdd = javaFunctions(sc)
				.cassandraTable("java_api", "summaries",
						mapRowTo(Summary.class)).keyBy(
						new Function<Summary, Integer>() {
							/**
					 * 
					 */
							private static final long serialVersionUID = 1L;

							@Override
							public Integer call(Summary summary)
									throws Exception {
								return summary.getProduct();
							}
						});

		JavaPairRDD<Integer, Product> productsRdd = javaFunctions(sc)
				.cassandraTable("java_api", "products", mapRowTo(Product.class))
				.keyBy(new Function<Product, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Product product) throws Exception {
						return product.getId();
					}
				});

		List<Tuple2<Product, Optional<Summary>>> results = productsRdd
				.leftOuterJoin(summariesRdd).values().toArray();

		for (Tuple2<Product, Optional<Summary>> result : results) {
			logger.info(result);
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.err
					.println("Syntax: com.datastax.spark.demo.JavaDemo <Cassandra contact point>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf();
		conf.setAppName("Java API demo");
		// conf.setMaster(args[0]);
		conf.set("spark.cassandra.connection.host", args[0]);
		JavaDemo app = new JavaDemo(conf);
		app.run();
	}
}
