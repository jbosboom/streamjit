package edu.mit.streamjit.impl.common;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.tuner.SqliteAdapter;
import edu.mit.streamjit.util.ConfigurationUtils;

public class ConfigurationAnalyzer {

	public static void main(String[] args) {
		ConfigurationAnalyzer ca = new ConfigurationAnalyzer(
				"NestedSplitJoinCore");
		// ca.compare(3, 4);

		System.out.println(ca.getRunningTime("NestedSplitJoinCore", 3));
	}

	private final String cfgDir;

	private final String appName;

	List<Integer> bestConfigurations;

	/**
	 * Path of the directory which contains app's configuration in sub
	 * directory.
	 * 
	 * <pre>
	 * confgDirectory
	 * 			|
	 * 			------>appName1
	 * 			|
	 * 			------>appName2
	 * 			|
	 * 			------>
	 * </pre>
	 */

	public ConfigurationAnalyzer(String appName) {
		verifyPath(ConfigurationUtils.configDir, appName);
		bestConfigurations = new LinkedList<>();
		this.appName = appName;
		this.cfgDir = String.format("%s%s%s", appName, File.separator,
				ConfigurationUtils.configDir);
	}

	private void compare(FloatParameter p1, FloatParameter p2) {
		float val1 = p1.getValue();
		float val2 = p2.getValue();
		if (val1 == val2)
			System.out.println(String.format("%s: p1 = p2. value = %f",
					p1.getName(), val1));
		if (val1 > val2)
			System.out.println(String.format("%s: p1 > p2. %f > %f",
					p1.getName(), val1, val2));
		else
			System.out.println(String.format("%s: p1 < p2. %f < %f",
					p1.getName(), val1, val2));
	}

	private void compare(Integer first, Integer second) {
		Configuration cfg1 = readcoConfiguration(first);
		Configuration cfg2 = readcoConfiguration(second);
		for (Entry<String, Parameter> en : cfg1.getParametersMap().entrySet()) {
			Parameter p1 = en.getValue();
			Parameter p2 = cfg2.getParameter(en.getKey());
			if (p2 == null)
				throw new IllegalStateException(String.format(
						"No parameter %s in configuration2", en.getKey()));
			if (p1.getClass() == Configuration.IntParameter.class)
				compare((IntParameter) p1, (IntParameter) p2);
			else if (p1.getClass() == Configuration.FloatParameter.class)
				compare((FloatParameter) p1, (FloatParameter) p2);
			else if (p1.getClass() == Configuration.SwitchParameter.class)
				compare((SwitchParameter<?>) p1, (SwitchParameter<?>) p2);
			else
				System.out.println(String.format(
						"Parameter class %s is not handled.", p1.getClass()
								.getName()));

		}
	}

	/*
	 * Any way to avoid code duplication in compare(IntParameter p1,
	 * IntParameter p2) and compare(FloatParameter p1, FloatParameter p2)?
	 */
	/**
	 * 
	 * @param p1
	 * @param p2
	 */
	private void compare(IntParameter p1, IntParameter p2) {
		int val1 = p1.getValue();
		int val2 = p2.getValue();
		if (val1 == val2)
			System.out.println(String.format("%s: p1 = p2. value = %d",
					p1.getName(), val1));
		if (val1 > val2)
			System.out.println(String.format("%s: p1 > p2. %d > %d",
					p1.getName(), val1, val2));
		else
			System.out.println(String.format("%s: p1 < p2. %d < %d",
					p1.getName(), val1, val2));
	}

	private <T1, T2> void compare(SwitchParameter<T1> p1, SwitchParameter<T2> p2) {
		Class<T1> type1 = p1.getGenericParameter();
		Class<T2> type2 = p2.getGenericParameter();
		assert type1 == type2;
		T1 val1 = p1.getValue();
		T2 val2 = p2.getValue();

		if (val1.equals(val2))
			System.out.println(String.format(
					"%s - same values - p1 = %s, p2 = %s. Universe:%s",
					p1.getName(), val1, val2, p1.getUniverse()));
		else
			System.out.println(String.format(
					"%s - different values - p1 = %s, p2 = %s. Universe:%s",
					p1.getName(), val1, val2, p1.getUniverse()));
	}

	private SqliteAdapter connectDB(String appName) {
		SqliteAdapter sqlite = new SqliteAdapter();
		sqlite.connectDB(appName);
		return sqlite;
	}

	private double getRunningTime(String appName, int round) {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		SqliteAdapter sqlite = connectDB(dbPath);
		ResultSet result = sqlite.executeQuery(String.format(
				"SELECT * FROM result WHERE id=%d", round));

		String runtime = "1000000000";
		try {
			runtime = result.getString("time");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Double.parseDouble(runtime);
	}

	private Configuration readcoConfiguration(Integer cfgNo) {
		String cfg = String.format("%s%s%d_%s.cfg", cfgDir, File.separator,
				cfgNo, appName);
		return ConfigurationUtils.readConfiguration(cfg);
	}

	private boolean verifyPath(String cfgDir, String appName) {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		File db = new File(dbPath);
		if (!db.exists())
			throw new IllegalStateException("No database file found in "
					+ dbPath);

		String dirPath = String.format("%s%s%s", appName, File.separator,
				cfgDir);
		File dir = new File(dirPath);
		if (!dir.exists())
			throw new IllegalStateException("No directory found in " + dirPath);

		return true;
	}
}
