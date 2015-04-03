package vn.wss.spark.recommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import vn.wss.spark.model.PModel;
import vn.wss.spark.model.RModel;
import com.google.common.base.Optional;

public class UpdateAB {
	private JavaRDD<PModel> input;

	public UpdateAB() {
		// TODO Auto-generated constructor stub
	}

	public UpdateAB(JavaRDD<PModel> input) {
		this.input = input;
	}

	public void run() {
		// TODO Auto-generated method stub
		// update A property
		JavaPairRDD<Long, Integer> i1 = input.mapToPair(
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
		//update to table Ratings
	}

}
