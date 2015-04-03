package vn.websosanh.sparkexample;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.IntegerType;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Optional;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import vn.wss.util.DateUtils;

public class asd {
	int i = 0;

	@Test
	public void test() {
		int a=1;
		abc name=new abc(a);
		name.get();
		System.out.println(a);
	}
	class abc{
		private int a;
		public abc(int a){
			this.a=a;
		}
		public void get(){
			this.a+=11;
		}
		public int getA() {
			return a;
		}
		public void setA(int a) {
			this.a = a;
		}
		
	}

}
