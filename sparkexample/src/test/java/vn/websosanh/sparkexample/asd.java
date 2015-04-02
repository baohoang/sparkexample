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

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import vn.wss.util.DateUtils;

public class asd {

	@Test
	public void test() {
		String url = "jdbc:sqlserver://183.91.14.82:1433;database=QT_2";
		String username = "qt_vn";
		String password = "@F4sJ=l9/ryJt9MT";
		try {
			Class.forName(SQLServerDriver.class.getName());
			Connection conn = DriverManager.getConnection(url, username,
					password);
			
			System.out.println(conn.getAutoCommit());
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
