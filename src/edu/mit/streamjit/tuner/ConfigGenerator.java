package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.CompilerBlobFactory;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.apps.bitonicsort.BitonicSort;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * ConfigGenerator generates {@link Configuration} of an application and stores
 * it into a database. Later, Opentuner can read this configuration for tuning.
 * In this way, Opentuner can start and stop the StreamJit app for each tuning
 * try so that Opentuner can tune JVM parameters such as heapsize,
 * inlinethreshold, GCpausetime, etc as well.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Sep 10, 2013
 */

public class ConfigGenerator {

	private String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}

	public void tune(BenchmarkProvider provider) throws InterruptedException,
			IOException {
		sqliteAdapter sqlite = new sqliteAdapter();
		// String dbPath = String.format(
		// "lib%sopentuner%sstreamjit%sstreamjit.db", File.separator,
		// File.separator, File.separator);

		String dbPath = "streamjit.db";
		sqlite.connectDB(dbPath);
		sqlite.createTable(
				"apps",
				"name string PRIMARY KEY NOT NULL, configuration string, location string, classname string");

		sqlite.createTable("FinalResult",
				"Name string, JVMOption string, SJConfig string, Round int, Exectime real");

		String jarFilePath = provider.getClass().getProtectionDomain()
				.getCodeSource().getLocation().getPath();

		String className = provider.getClass().getName();

		Benchmark app = provider.iterator().next();
		// TODO: BlobFactory.getDefaultConfiguration() asks for workers. But
		// from outside workers are not available. need to do something
		// else. This is not a proper design to do.
		BlobFactory bf = new CompilerBlobFactory();
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		OneToOneElement<?, ?> stream = app.instantiate();
		stream.visit(cwv);
		ImmutableSet<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv
				.getSource());
		Configuration cfg = bf.getDefaultConfiguration(workers);

		String name = app.toString();
		String confString = getConfigurationString(cfg);

		try {
			sqlite.executeUpdate(String.format(
					"DELETE FROM apps WHERE name='%s'", name));
		} catch (Exception ex) {
			// Entry not found.
		}

		sqlite.executeUpdate(String.format(
				"insert into apps values('%s', '%s', '%s', '%s')", name,
				confString, jarFilePath, className));

		String tunerPath = String.format(
				"lib%sopentuner%sstreamjit%stuner2.py", File.separator,
				File.separator, File.separator);

		// new ProcessBuilder("xterm", "-e", "python", tunerPath).start();
	}

	public static class sqliteAdapter {

		private Statement statement;
		private Connection con = null;

		public sqliteAdapter() {
			try {
				Class.forName("org.sqlite.JDBC");
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
		}

		public void connectDB(String path) {
			try {
				con = DriverManager.getConnection(String.format(
						"jdbc:sqlite:%s", path));
				statement = con.createStatement();
				statement.setQueryTimeout(30); // set timeout to 30 sec.

			} catch (SQLException e) {
				// TODO Auto-generated catch block
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
					statement.executeUpdate(String.format(
							"create table %s (%s)", table, signature));
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

	/**
	 * @param args
	 *            [0] - String topLevelWorkerName args[1] - String jarFilePath
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {
		// BenchmarkProvider provider = new ChannelVocoder7();
		// BenchmarkProvider provider = new FMRadio.FMRadioBenchmarkProvider();
		BenchmarkProvider provider = new BitonicSort();
		// BenchmarkProvider provider = new FileInputSanity();
		// BenchmarkProvider provider = new SplitjoinOrderSanity();
		// BenchmarkProvider provider = new HelperFunctionSanity();

		ConfigGenerator cfgGen = new ConfigGenerator();
		cfgGen.tune(provider);
	}
}