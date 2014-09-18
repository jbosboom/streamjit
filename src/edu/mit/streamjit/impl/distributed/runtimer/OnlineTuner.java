package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.tuner.OpenTuner;
import edu.mit.streamjit.tuner.TCPTuner;
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
	private final StreamJitApp app;
	private final ConfigurationManager cfgManager;
	private final boolean needTermination;

	public OnlineTuner(AbstractDrainer drainer, StreamJitAppManager manager,
			StreamJitApp app, ConfigurationManager cfgManager,
			boolean needTermination) {
		this.drainer = drainer;
		this.manager = manager;
		this.app = app;
		this.cfgManager = cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
	}

	@Override
	public void run() {
		if (GlobalConstants.tune == 1)
			tune();
		else if (GlobalConstants.tune == 2)
			evaluate();
		else
			System.err
					.println("GlobalConstants.tune is neither in tune mode nor in evaluate mode.");
	}

	private void tune() {
		int round = 0;
		try {
			tuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
					File.separator, File.separator));

			tuner.writeLine("program");
			tuner.writeLine(app.name);

			tuner.writeLine("confg");
			tuner.writeLine(Jsonifiers.toJson(app.blobConfiguration).toString());

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

				System.out.println(String.format(
						"---------------------%d-------------------------",
						++round));
				Configuration config = Configuration.fromJson(cfgJson);

				if (GlobalConstants.saveAllConfigurations)
					saveConfg(cfgJson, round);

				ret = reconfigure(config);
				if (ret.first) {
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

		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (needTermination)
			terminate();
	}

	private void evaluate() {
		Configuration finalCfg = readConfiguration(String.format(
				"final_%s.cfg", app.name));
		evaluateConfig(finalCfg, "Final configuration");

		Configuration handCfg = readConfiguration(String.format("hand_%s.cfg",
				app.name));
		evaluateConfig(handCfg, "Handtuned configuration");

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
		try {
			if (!cfgManager.newConfiguration(config)) {
				return new Pair<Boolean, Long>(true, -1l);
			}

			if (manager.isRunning()) {
				boolean state = drainer.startDraining(0);
				if (!state) {
					System.err
							.println("Final drain has already been called. no more tuning.");
					return new Pair<Boolean, Long>(false, -1l);
				}

				System.err.println("awaitDrainedIntrmdiate");
				drainer.awaitDrainedIntrmdiate();

				if (GlobalConstants.useDrainData) {
					System.err.println("awaitDrainData...");
					drainer.awaitDrainData();
					DrainData drainData = drainer.getDrainData();
					app.drainData = drainData;
				}
			}

			int multiplier = 1000;
			IntParameter mulParam = config.getParameter("multiplier",
					IntParameter.class);
			if (mulParam != null)
				multiplier = mulParam.getValue();

			drainer.setBlobGraph(app.blobGraph);
			System.err.println("Reconfiguring...multiplier = " + multiplier);
			if (manager.reconfigure(multiplier)) {
				// TODO: need to check the manager's status before
				// passing
				// the time. Exceptions, final drain, etc may causes app
				// to
				// stop executing.
				time = manager.getFixedOutputTime();

				System.out.println("Execution time is " + time
						+ " milli seconds");
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
	 * Just excerpted from run() method for better readability.
	 * 
	 * @throws IOException
	 */
	private void handleTermination() throws IOException {
		String finalConfg = tuner.readLine();
		System.out.println("Tuning finished");
		saveConfg(finalConfg, 0);

		Configuration finalcfg = Configuration.fromJson(finalConfg);
		evaluateConfig(finalcfg, "Final configuration");

		Configuration handCfg = readConfiguration(String.format("hand_%s.cfg",
				app.name));
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
			drainer.startDraining(1);
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
		int count = 8;
		try {
			writer = new FileWriter(String.format("Eval_%s.txt", app.name),
					true);
			writer.write("\n----------------------------------------\n");
			writer.write(String.format("Configuration name = %s\n", cfgName));
			if (cfg != null) {
				Pair<Boolean, Long> ret = reconfigure(cfg); // often the first
															// run shows huge
															// noise.
				for (int i = 0; i < count; i++) {
					ret = reconfigure(cfg);
					writer.write(ret.second.toString());
					writer.write('\n');
					writer.flush();
					total += ret.second;
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

	/**
	 * Save the configuration.
	 */
	private void saveConfg(String json, int round) {
		try {

			File dir = new File(String.format("configurations%s%s",
					File.separator, app.name));
			if (!dir.exists())
				if (!dir.mkdirs()) {
					System.err.println("Make directory failed");
					return;
				}

			File file = new File(dir,
					String.format("%d%s.cfg", round, app.name));
			FileWriter writer = new FileWriter(file, false);
			writer.write(json);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}