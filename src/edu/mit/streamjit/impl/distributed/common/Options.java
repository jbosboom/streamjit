package edu.mit.streamjit.impl.distributed.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConnectionManager.AllConnectionParams;
import edu.mit.streamjit.impl.distributed.ConnectionManager.AsyncTCPNoParams;
import edu.mit.streamjit.impl.distributed.ConnectionManager.BlockingTCPNoParams;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.TailChannels;
import edu.mit.streamjit.tuner.TCPTuner;

/**
 * Program options. Loads the values from "options.properties".
 * 
 * @author sumanan
 * @since 1 Mar, 2015
 */
public final class Options {

	/**
	 * We can set this value at class loading time also as follows.
	 * 
	 * <code>maxThreadCount = Math.max(Runtime.getntime().availableProcessors() / 2,
	 * 1);</code>
	 * 
	 * Lets hard code this for the moment.
	 */
	public static final int maxNumCores;

	/**
	 * To turn on or off the dead lock handler. see {@link AbstractDrainer} for
	 * it's usage.
	 */
	public static final boolean needDrainDeadlockHandler;

	/**
	 * Turn On/Off the profiling.
	 */
	public static final boolean needProfiler;

	/**
	 * Output count for tuning. Tuner measures the running time for this number
	 * of outputs.
	 */
	public static final int outputCount;

	/**
	 * Period to print output count periodically. This printing feature get
	 * turned off if this value is less than 1. Time unit is ms. See
	 * {@link TailChannels}.
	 */
	public static final int printOutputCountPeriod;

	/**
	 * Save all configurations tired by open tuner in to
	 * "configurations//app.name" directory.
	 */
	public static final boolean saveAllConfigurations;

	/**
	 * Enables {@link DistributedStreamCompiler} to run on a single node. When
	 * this is enabled, noOfNodes passed as compiler argument has no effect.
	 */
	public static final boolean singleNodeOnline;

	/**
	 * Enables tuning. Tuner will be started iff this flag is set true.
	 * Otherwise, just use the fixed configuration file to run the program. No
	 * tuning, no intermediate draining. In this mode (tune = false), time taken
	 * to pass fixed number of input will be measured for 30 rounds and logged
	 * into FixedOutPut.txt. See {@link TailChannels} for the file logging
	 * details.
	 * <ol>
	 * 0 - No tuning, uses configuration file to run.
	 * <ol>
	 * 1 - Tuning.
	 * <ol>
	 * 2 - Evaluate configuration files. ( compares final cfg with hand tuned
	 * cfg. Both file should be presented in the running directory.
	 */
	public static final int tune;

	/**
	 * Decides how to start the opentuner. In first 2 cases, controller starts
	 * opentuner and establishes connection with it on a random port no range
	 * from 5000-65536. User can provide port no in 3 case.
	 * 
	 * <ol>
	 * <li>0 - Controller starts the tuner automatically on a terminal. User can
	 * see Opentuner related outputs in the new terminal.
	 * <li>1 - Controller starts the tuner automatically as a Python process. No
	 * explicit window will be opened. Suitable for remote running through SSH
	 * terminal.
	 * <li>2 - User has to manually start the tuner with correct portNo as
	 * argument. Port no 12563 is used in this case. But it can be changed at
	 * {@link TCPTuner#startTuner(String)}. We need this option to run the
	 * tuning on remote machines.
	 * </ol>
	 */
	public static final int tunerStartMode;

	/**
	 * if true uses Compiler2, interpreter otherwise.
	 */
	public static final boolean useCompilerBlob;

	/**
	 * To turn on or turn off the drain data. If this is false, drain data will
	 * be ignored and every new reconfiguration will run with fresh inputs.
	 */
	public static final boolean useDrainData;

	// Following are miscellaneous options to avoid rebuilding jar files every
	// time to change some class selections.
	// TODO: Fix all design pattern related issues.

	/**
	 * <ol>
	 * <li>0 - {@link AllConnectionParams}
	 * <li>1 - {@link BlockingTCPNoParams}
	 * <li>2 - {@link AsyncTCPNoParams}
	 * <li>default: {@link AsyncTCPNoParams}
	 * </ol>
	 */
	public static final int connectionManager;

	static {
		Properties prop = loadProperties();
		printOutputCountPeriod = Integer.parseInt(prop
				.getProperty("printOutputCountPeriod"));;
		maxNumCores = Integer.parseInt(prop.getProperty("maxNumCores"));
		useCompilerBlob = Boolean.parseBoolean(prop
				.getProperty("useCompilerBlob"));
		needDrainDeadlockHandler = Boolean.parseBoolean(prop
				.getProperty("needDrainDeadlockHandler"));
		needProfiler = Boolean.parseBoolean(prop.getProperty("needProfiler"));
		outputCount = Integer.parseInt(prop.getProperty("outputCount"));
		tune = Integer.parseInt(prop.getProperty("tune"));
		tunerStartMode = Integer.parseInt(prop.getProperty("tunerStartMode"));
		saveAllConfigurations = Boolean.parseBoolean(prop
				.getProperty("saveAllConfigurations"));
		singleNodeOnline = Boolean.parseBoolean(prop
				.getProperty("singleNodeOnline"));
		useDrainData = Boolean.parseBoolean(prop.getProperty("useDrainData"));
		connectionManager = Integer.parseInt(prop
				.getProperty("connectionManager"));
	}

	public static Properties getProperties() {
		Properties prop = new Properties();
		setProperty(prop, "tunerStartMode", tunerStartMode);
		setProperty(prop, "useDrainData", useDrainData);
		setProperty(prop, "needDrainDeadlockHandler", needDrainDeadlockHandler);
		setProperty(prop, "tune", tune);
		setProperty(prop, "saveAllConfigurations", saveAllConfigurations);
		setProperty(prop, "outputCount", outputCount);
		setProperty(prop, "useCompilerBlob", useCompilerBlob);
		setProperty(prop, "printOutputCountPeriod", printOutputCountPeriod);
		setProperty(prop, "singleNodeOnline", singleNodeOnline);
		setProperty(prop, "maxNumCores", maxNumCores);
		setProperty(prop, "needProfiler", needProfiler);
		setProperty(prop, "connectionManager", connectionManager);
		return prop;
	}

	private static Properties loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("options.properties");
			prop.load(input);
		} catch (IOException ex) {
			System.err.println("Failed to load options.properties");
		}
		return prop;
	}

	private static void setProperty(Properties prop, String name, Boolean val) {
		prop.setProperty(name, val.toString());
	}

	private static void setProperty(Properties prop, String name, Integer val) {
		prop.setProperty(name, val.toString());
	}

	public static void storeProperties() {
		OutputStream output = null;
		try {
			output = new FileOutputStream("options.properties");
			Properties prop = getProperties();
			prop.store(output, null);
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
