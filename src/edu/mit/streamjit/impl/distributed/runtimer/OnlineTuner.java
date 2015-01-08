package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.tuner.OpenTuner;
import edu.mit.streamjit.tuner.TCPTuner;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;
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
		this.prognosticator = new GraphPropertyPrognosticator(app, cfgManager);
	}

	@Override
	public void run() {
		if (GlobalConstants.tune == 1)
			tune();
		else if (GlobalConstants.tune == 2)
			// verifyTuningTimes();
			evaluate();
		else
			System.err
					.println("GlobalConstants.tune is neither in tune mode nor in evaluate mode.");
	}

	private void tune() {
		int round = 0;
		try {
			startTuner();
			Pair<Boolean, Long> ret;

			System.out.println("New tune run.............");
			while (manager.getStatus() != AppStatus.STOPPED) {
				String cfgJson = tuner.readLine();
				logger.newConfiguration();
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

				System.out.println(String.format(
						"---------------------%d-------------------------",
						++round));
				Configuration config = Configuration.fromJson(cfgJson);
				config = ConfigurationUtils.addConfigPrefix(config,
						new Integer(round).toString());

				if (GlobalConstants.saveAllConfigurations)
					ConfigurationUtils.saveConfg(cfgJson,
							new Integer(round).toString(), app.name);

				boolean possibleBetter = prognosticator.prognosticate(config);
				if (possibleBetter) {
					ret = reconfigure(config);
					if (ret.first) {
						prognosticator.time(ret.second);
						tuner.writeLine(new Double(ret.second).toString());
					} else {
						tuner.writeLine("exit");
						break;
					}
				} else
					tuner.writeLine(new Double(-1).toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
			terminate();
		}

		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (needTermination)
			terminate();
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

	private void evaluate() {
		Configuration finalCfg = ConfigurationUtils.readConfiguration(app.name,
				"final");
		evaluateConfig(finalCfg, "Final configuration");

		Configuration handCfg = ConfigurationUtils.readConfiguration(app.name,
				"hand");
		evaluateConfig(handCfg, "Handtuned configuration");

		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}
		terminate();
	}

	/**
	 * This method just picks a few configurations and re-run the app to ensure
	 * the time we reported to the opentuner is correct.
	 * 
	 * This method can be called after the completion of the tuning.
	 */
	private void verifyTuningTimes() {
		int[] cfgNos = { 10, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500,
				550, 600, 650, 700, 750, 800, 850, 900, 950, 1000 };
		for (int n : cfgNos) {
			String cfgName = String.format("%d_%s.cfg", n, app.name);
			Configuration cfg = ConfigurationUtils.readConfiguration(app.name,
					new Integer(n).toString());

			if (cfg == null)
				continue;
			evaluateConfig(cfg, cfgName);
		}
		terminate();
	}

	/**
	 * @param cfgJson
	 * @param round
	 * @return if ret.first == false, then no more tuning. ret.second = running
	 *         time in milliseconds.
	 */
	private Pair<Boolean, Long> reconfigure(Configuration config) {
		long time;

		if (manager.getStatus() == AppStatus.STOPPED)
			return new Pair<Boolean, Long>(false, 0l);

		try {
			if (!cfgManager.newConfiguration(config))
				return new Pair<Boolean, Long>(true, -1l);

			if (!intermediateDraining())
				return new Pair<Boolean, Long>(false, -1l);

			drainer.setBlobGraph(app.blobGraph);
			int multiplier = getMultiplier(config);
			if (manager.reconfigure(multiplier)) {
				// TODO: need to check the manager's status before passing the
				// time. Exceptions, final drain, etc may causes app to stop
				// executing.
				time = manager.getFixedOutputTime();
				logger.logRunTime(time);
			} else {
				time = -1l;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err
					.println("Couldn't compile the stream graph with this configuration");
			time = -1l;
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
			Pair<Boolean, Long> ret = reconfigure(finalcfg);
			if (ret.first && ret.second > 0)
				System.out
						.println("Application is running forever with the final configuration.");
			else
				System.err.println("Invalid final configuration.");
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
		int count = 4;
		try {
			writer = new FileWriter(String.format("%s%sEval_%s.txt", app.name,
					File.separator, app.name), true);
			writer.write("\n----------------------------------------\n");
			writer.write(String.format("Configuration name = %s\n", cfgName));
			if (cfg != null) {
				Pair<Boolean, Long> ret;
				for (int i = 0; i < count; i++) {
					ret = reconfigure(cfg);
					if (ret.first) {
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
}