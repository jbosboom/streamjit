package edu.mit.streamjit.impl.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;

public class ConfigurationAnalyzer {

	public static void main(String[] args) {
		ConfigurationAnalyzer ca = new ConfigurationAnalyzer(
				"NestedSplitJoinCore");
		ca.compare(3, 4);
	}

	String appDir;

	String appName;

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

	private final String cfgDirectory = "configurations";

	public ConfigurationAnalyzer(String appName) {
		verifyPath(cfgDirectory, appName);
		bestConfigurations = new LinkedList<>();
		this.appName = appName;
		this.appDir = String.format("%s%s%s", cfgDirectory, File.separator,
				appName);
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
		Configuration cfg1 = readcoConfiguration(appDir, appName, first);
		Configuration cfg2 = readcoConfiguration(appDir, appName, second);
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

	private float getRunningTime() {
		return 1.0f;
	}

	private Configuration readcoConfiguration(String appDir, String appName,
			Integer cfgNo) {
		String cfg = String.format("%s%s%d%s.cfg", appDir, File.separator,
				cfgNo, appName);
		return readConfiguration(cfg);
	}

	private Configuration readConfiguration(String name) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(name));
			String json = reader.readLine();
			reader.close();
			return Configuration.fromJson(json);
		} catch (Exception ex) {
			System.err.println(String.format(
					"File reader error. No %s configuration file.", name));
		}
		return null;
	}

	private boolean verifyPath(String cfgDir, String appName) {
		String dbPath = appName;
		File db = new File(dbPath);
		if (!db.exists())
			throw new IllegalStateException("No database file found in "
					+ dbPath);

		String dirPath = String.format("%s%s%s", cfgDir, File.separator,
				appName);
		File dir = new File(dirPath);
		if (!dir.exists())
			throw new IllegalStateException("No directory found in " + dirPath);

		return true;
	}
}
