package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.tuner.OpenTuner;
import edu.mit.streamjit.tuner.TCPTuner;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.TimeLogProcessor;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * Online tuner does continues learning.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 8, 2013
 */
public class OnlineTuner implements Runnable {
	private final AbstractDrainer drainer;
	private final StreamJitAppManager manager;
	private final OpenTuner tuner;
	private final StreamJitApp<?, ?> app;
	private final ConfigurationManager cfgManager;
	private final boolean needTermination;
	private final TimeLogger logger;
	private final ConfigurationPrognosticator prognosticator;

	public OnlineTuner(AbstractDrainer drainer, StreamJitAppManager manager,
			StreamJitApp<?, ?> app, ConfigurationManager cfgManager,
			TimeLogger logger, boolean needTermination) {
		this.drainer = drainer;
		this.manager = manager;
		this.app = app;
		this.cfgManager = cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
		this.logger = logger;
		this.prognosticator = new GraphPropertyPrognosticator(app);
	}

	@Override
	public void run() {
		if (GlobalConstants.tune == 1)
			tune();
		else if (GlobalConstants.tune == 2)
			verifyTuningTimes(cfgPrefixes());
		else
			System.err
					.println("GlobalConstants.tune is neither in tune mode nor in evaluate mode.");
	}

	private void tune() {
		int round = 0;
		// Keeps track of the current best time. Uses this to discard bad cfgs
		// early.
		long currentBestTime = Long.MAX_VALUE;
		try {
			startTuner();
			Pair<Boolean, Long> ret;

			System.out.println("New tune run.............");
			while (manager.getStatus() != AppStatus.STOPPED) {
				String cfgJson = tuner.readLine();
				if (cfgJson == null) {
					System.err.println("OpenTuner closed unexpectly.");
					break;
				}

				// At the end of the tuning, Opentuner will send "Completed"
				// msg. This means no more tuning.
				if (cfgJson.equals("Completed")) {
					handleTermination();
					break;
				}

				Configuration config = newCfg(++round, cfgJson);
				ret = reconfigure(config, 2 * currentBestTime);
				if (ret.first) {
					long time = ret.second;
					currentBestTime = (time > 1 && currentBestTime > time)
							? time : currentBestTime;
					prognosticator.time(ret.second);
					tuner.writeLine(new Double(ret.second).toString());
				} else {
					tuner.writeLine("exit");
					break;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
			terminate();
		}

		tuningFinished();
	}

	private void startTuner() throws IOException {
		String relativeTunerPath = String.format(
				"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
				File.separator, File.separator);

		String absoluteTunerPath = String.format("%s%s%s",
				System.getProperty("user.dir"), File.separator,
				relativeTunerPath);

		tuner.startTuner(absoluteTunerPath, new File(app.name));

		tuner.writeLine("program");
		tuner.writeLine(app.name);

		tuner.writeLine("confg");
		tuner.writeLine(Jsonifiers.toJson(app.getConfiguration()).toString());
	}

	/**
	 * This method just picks a few configurations and re-run the app to ensure
	 * the time we reported to the opentuner is correct.
	 * 
	 * This method can be called after the completion of the tuning.
	 */
	private void verifyTuningTimes(Iterable<String> cfgPrefixes) {
		for (String prefix : cfgPrefixes) {
			String cfgName = String.format("%s_%s.cfg", prefix, app.name);
			Configuration cfg = ConfigurationUtils.readConfiguration(app.name,
					prefix);
			if (cfg == null) {
				System.err.println(String.format("No %s file exists", cfgName));
				continue;
			}
			cfg = ConfigurationUtils.addConfigPrefix(cfg, prefix);
			evaluateConfig(cfg, cfgName);
		}

		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}
		terminate();
	}

	/**
	 * TODO: Split this method into two methods, 1.reconfigure(),
	 * 2.getFixedOutputTime().
	 * 
	 * @param cfgJson
	 * @param round
	 * @return if ret.first == false, then no more tuning. ret.second = running
	 *         time in milliseconds. ret.second may be a negative value if the
	 *         reconfiguration is unsuccessful or a timeout is occurred.
	 *         Meanings of the negative values are follows
	 *         <ol>
	 *         <li>-1: Timeout has occurred.
	 *         <li>-2: Invalid configuration.
	 *         <li>-3: {@link ConfigurationPrognosticator} has rejected the
	 *         configuration.
	 *         <li>-4: Draining failed. Another draining is in progress.
	 *         <li>-5: Reconfiguration has failed at {@link StreamNode} side.
	 *         E.g., Compilation error.
	 *         <li>-6: Misc problems.
	 */
	private Pair<Boolean, Long> reconfigure(Configuration config, long timeout) {
		long time;

		if (manager.getStatus() == AppStatus.STOPPED)
			return new Pair<Boolean, Long>(false, 0l);

		if (!cfgManager.newConfiguration(config))
			return new Pair<Boolean, Long>(true, -2l);

		if (!prognosticator.prognosticate(config))
			return new Pair<Boolean, Long>(true, -3l);

		try {
			if (!intermediateDraining())
				return new Pair<Boolean, Long>(false, -4l);

			drainer.setBlobGraph(app.blobGraph);
			int multiplier = getMultiplier(config);
			if (manager.reconfigure(multiplier)) {
				// TODO: need to check the manager's status before passing the
				// time. Exceptions, final drain, etc may causes app to stop
				// executing.
				time = manager.getFixedOutputTime(timeout);
				logger.logRunTime(time);
			} else {
				time = -5l;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err
					.println("Couldn't compile the stream graph with this configuration");
			time = -6l;
		}
		return new Pair<Boolean, Long>(true, time);
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 * @throws InterruptedException
	 */
	private boolean intermediateDraining() throws InterruptedException {
		if (manager.isRunning()) {
			return drainer.drainIntermediate();
		} else
			return true;
	}

	private int getMultiplier(Configuration config) {
		int multiplier = 50;
		IntParameter mulParam = config.getParameter("multiplier",
				IntParameter.class);
		if (mulParam != null)
			multiplier = mulParam.getValue();
		System.err.println("Reconfiguring...multiplier = " + multiplier);
		return multiplier;
	}

	/**
	 * Just excerpted from run() method for better readability.
	 * 
	 * @throws IOException
	 */
	private void handleTermination() throws IOException {
		String finalConfg = tuner.readLine();
		System.out.println("Tuning finished");
		ConfigurationUtils.saveConfg(finalConfg, "final", app.name);
		Configuration finalcfg = Configuration.fromJson(finalConfg);
		finalcfg = ConfigurationUtils.addConfigPrefix(finalcfg, "final");
		evaluateConfig(finalcfg, "Final configuration");

		Configuration handCfg = ConfigurationUtils.readConfiguration(app.name,
				"hand");
		handCfg = ConfigurationUtils.addConfigPrefix(handCfg, "hand");
		evaluateConfig(handCfg, "Handtuned configuration");

		if (needTermination) {
			terminate();
		} else {
			Pair<Boolean, Long> ret = reconfigure(finalcfg, 0);
			if (ret.first && ret.second > 0)
				System.out
						.println("Application is running forever with the final configuration.");
			else
				System.err.println("Invalid final configuration.");
		}
	}

	private Configuration newCfg(int round, String cfgJson) {
		String cfgPrefix = new Integer(round).toString();
		System.out.println(String.format(
				"---------------------%s-------------------------", cfgPrefix));
		logger.newConfiguration(cfgPrefix);
		Configuration config = Configuration.fromJson(cfgJson);
		config = ConfigurationUtils.addConfigPrefix(config, cfgPrefix);

		if (GlobalConstants.saveAllConfigurations)
			ConfigurationUtils.saveConfg(cfgJson, cfgPrefix, app.name);
		return config;
	}

	private void tuningFinished() {
		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (needTermination)
			terminate();

		try {
			TimeLogProcessor.summarize(app.name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void terminate() {
		if (manager.isRunning()) {
			// drainer.startDraining(1);
			drainer.drainFinal(true);
		} else {
			manager.stop();
		}
	}

	/**
	 * Evaluates a configuration.
	 * 
	 * @param cfg
	 *            configuration that needs to be evaluated
	 * @param cfgName
	 *            name of the configuration. This is just for logging purpose.
	 */
	private void evaluateConfig(Configuration cfg, String cfgName) {
		System.out.println("Evaluating " + cfgName);
		FileWriter writer;
		double total = 0;
		int count = 2;
		try {
			writer = new FileWriter(String.format("%s%sEval_%s.txt", app.name,
					File.separator, app.name), true);
			writer.write("\n----------------------------------------\n");
			writer.write(String.format("Configuration name = %s\n", cfgName));
			if (cfg != null) {
				Pair<Boolean, Long> ret;
				for (int i = 0; i < count; i++) {
					logger.newConfiguration(cfgName);
					ret = reconfigure(cfg, 0);
					if (ret.first) {
						prognosticator.time(ret.second);
						writer.write(ret.second.toString());
						writer.write('\n');
						writer.flush();
						total += ret.second;
					} else {
						break;
					}
				}
				double avg = total / count;
				writer.write(String.format("Average execution time = %f%n\n",
						avg));
			} else {
				writer.write("Null configuration\n");
			}
			writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private Iterable<String> cfgPrefixes() {
		List<String> cfgPrefixes = new ArrayList<String>();
		cfgPrefixes.add("final");
		cfgPrefixes.add("hand");
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					String.format("%s%sverify.txt", app.name, File.separator)));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] arr = line.split(",");
				for (String s : arr) {
					cfgPrefixes.add(s.trim());
				}
			}
			reader.close();
		} catch (IOException e) {
			// e.printStackTrace();
		}
		return cfgPrefixes;
	}
}