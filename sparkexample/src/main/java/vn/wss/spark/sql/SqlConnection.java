package vn.wss.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

public class SqlConnection {
	private Connection conn;
	private static SqlConnection instance = null;

	public static SqlConnection getInstance() {
		if (instance == null) {
			instance = new SqlConnection();
		}
		return instance;
	}

	public SqlConnection() {
		String url = "jdbc:sqlserver://183.91.14.82:1433;database=QT_2";
		String username = "qt_vn";
		String password = "@F4sJ=l9/ryJt9MT";
		try {
			Class.forName(SQLServerDriver.class.getName());
			conn = DriverManager.getConnection(url, username, password);

		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}
