package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqliteAdapter {

	private Connection con = null;
	private Statement statement;

	public SqliteAdapter() throws ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");
	}

	public void connectDB(String path) {
		try {
			con = DriverManager.getConnection(String.format("jdbc:sqlite:%s",
					path));
			statement = con.createStatement();
			statement.setQueryTimeout(30); // set timeout to 30 sec.

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates table iff it is not exists
	 * 
	 * @param table
	 *            Name of the table
	 * @param signature
	 *            Column format of the table
	 * @throws SQLException
	 */
	public void createTable(String table, String signature) {
		checkNotNull(con);
		DatabaseMetaData dbm;
		try {
			dbm = con.getMetaData();

			ResultSet tables = dbm.getTables(null, null, table, null);
			if (!tables.next()) {
				// "create table %s ()"
				statement.executeUpdate(String.format("create table %s (%s)",
						table, signature));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ResultSet executeQuery(String sql) {
		try {
			ResultSet rs = statement.executeQuery(sql);
			// con.commit();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int executeUpdate(String sql) {
		try {
			int ret = statement.executeUpdate(sql);
			// con.commit();
			return ret;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}
}