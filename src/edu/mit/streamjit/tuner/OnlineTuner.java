package edu.mit.streamjit.tuner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.tuner.MethodTimeLogger.FileMethodTimeLogger;
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
	private final Verifier verifier;
	private final MethodTimeLogger mLogger;
	private final Reconfigurer configurer;

	public OnlineTuner(Reconfigurer configurer, boolean needTermination) {
		this.configurer = configurer;
		this.drainer = configurer.drainer;
		this.manager = configurer.manager;
		this.app = configurer.app;
		this.cfgManager = configurer.cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
		this.logger = configurer.logger;
		this.prognosticator = new GraphPropertyPrognosticator(app);
		this.verifier = new Verifier();
		this.mLogger = new FileMethodTimeLogger(app.name);
	}

	@Override
	public void run() {
		if (Options.tune == 1)
			tune();
		else if (Options.tune == 2) {
			verifier.verify();
			terminate();
		} else
			System.err
					.println("GlobalConstants.tune is neither in tune mode nor in evaluate mode.");
	}

	private void tune() {
		int round = 0;
		// Keeps track of the current best time. Uses this to discard bad cfgs
		// early.
		long currentBestTime;
		if (Options.timeOut)
			currentBestTime = Long.MAX_VALUE;
		else
			currentBestTime = 0;
		Stopwatch searchTimeSW = Stopwatch.createStarted();
		try {
			mLogger.bStartTuner();
			startTuner();
			mLogger.eStartTuner();
			Pair<Boolean, Long> ret;

			System.out.println("New tune run.............");
			while (manager.getStatus() != AppStatus.STOPPED) {
				mLogger.bTuningRound();
				String cfgJson = tuner.readLine();
				logger.logSearchTime(searchTimeSW
						.elapsed(TimeUnit.MILLISECONDS));
				if (cfgJson == null) {
					System.err.println("OpenTuner closed unexpectly.");
					break;
				}

				// At the end of the tuning, Opentuner will send "Completed"
				// msg. This means no more tuning.
				if (cfgJson.equals("Completed")) {
					mLogger.bHandleTermination();
					handleTermination();
					mLogger.eHandleTermination();
					break;
				}

				mLogger.bNewCfg();
				Configuration config = newCfg(++round, cfgJson);
				mLogger.eNewCfg(round);
				mLogger.bReconfigure();
				ret = configurer.reconfigure(config, 2 * currentBestTime);
				mLogger.eReconfigure();
				if (ret.first) {
					long time = ret.second;
					currentBestTime = (time > 1 && currentBestTime > time)
							? time : currentBestTime;
					prognosticator.time(ret.second);
					tuner.writeLine(new Double(ret.second).toString());
					searchTimeSW.reset();
					searchTimeSW.start();
				} else {
					tuner.writeLine("exit");
					break;
				}
				mLogger.eTuningRound();
			}

		} catch (IOException e) {
			e.printStackTrace();
			mLogger.bTerminate();
			terminate();
			mLogger.eTerminate();
		}
		mLogger.bTuningFinished();
		tuningFinished();
		mLogger.eTuningFinished();
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

		verifier.verify();

		if (needTermination) {
			terminate();
		} else {
			Pair<Boolean, Long> ret = configurer.reconfigure(finalcfg, 0);
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

		if (Options.saveAllConfigurations)
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

	private class Verifier {

		public void verify() {
			verifyTuningTimes(cfgPrefixes());
		}

		/**
		 * This method just picks a few configurations and re-run the app to
		 * ensure the time we reported to the opentuner is correct.
		 * 
		 * This method can be called after the completion of the tuning.
		 * 
		 * @param cfgPrefixes
		 *            map of cfgPrefixes and expected running time.
		 */
		private void verifyTuningTimes(Map<String, Integer> cfgPrefixes) {
			try {
				FileWriter writer = writer();
				for (int i = 0; i < Options.verificationCount; i++) {
					for (Map.Entry<String, Integer> en : cfgPrefixes.entrySet()) {
						String prefix = en.getKey();
						Integer expectedRunningTime = en.getValue();
						String cfgName = String.format("%s_%s.cfg", prefix,
								app.name);
						Configuration cfg = ConfigurationUtils
								.readConfiguration(app.name, prefix);
						if (cfg == null) {
							System.err.println(String.format(
									"No %s file exists", cfgName));
							continue;
						}
						cfg = ConfigurationUtils.addConfigPrefix(cfg, prefix);
						writer.write("----------------------------------------\n");
						writer.write(String.format("Configuration name = %s\n",
								cfgName));
						List<Long> runningTimes = evaluateConfig(cfg, cfgName);
						processRunningTimes(runningTimes, expectedRunningTime,
								writer);
					}
				}
				writer.write("**************FINISHED**************\n\n");
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void processRunningTimes(List<Long> runningTimes,
				Integer expectedRunningTime, FileWriter writer)
				throws IOException {
			writer.write(String.format("Expected running time = %dms\n",
					expectedRunningTime));
			int correctEval = 0;
			double total = 0;
			for (int i = 0; i < runningTimes.size(); i++) {
				long runningTime = runningTimes.get(i);
				if (runningTime > 0) {
					correctEval++;
					total += runningTime;
				}
				writer.write(String.format("Evaluation %d = %dms\n", i + 1,
						runningTime));
			}
			double avg = total / correctEval;
			writer.write(String.format("Average running time = %.3fms\n", avg));
		}

		private FileWriter writer() throws IOException {
			FileWriter writer = new FileWriter(String.format(
					"%s%sevaluation.txt", app.name, File.separator, app.name),
					true);
			writer.write("##########################################################");
			Properties prop = Options.getProperties();
			prop.store(writer, "");
			return writer;
		}

		private Map<String, Integer> cfgPrefixes() {
			Map<String, Integer> cfgPrefixes = new HashMap<>();
			cfgPrefixes.put("final", 0);
			cfgPrefixes.put("hand", 0);
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						String.format("%s%sverify.txt", app.name,
								File.separator)));
				String line;
				while ((line = reader.readLine()) != null) {
					if (line.contains("="))
						process1(line, cfgPrefixes);
					else
						process2(line, cfgPrefixes);
				}
				reader.close();
			} catch (IOException e) {
			}
			return cfgPrefixes;
		}

		/**
		 * Processes the line that is generated by {@link TimeLogProcessor}.
		 */
		private void process1(String line, Map<String, Integer> cfgPrefixes) {
			String[] arr = line.split("=");
			String cfgPrefix = arr[0].trim();
			int expectedTime = 0;
			if (arr.length > 1)
				try {
					expectedTime = Integer.parseInt(arr[1]);
				} catch (NumberFormatException ex) {
					System.err.println("NumberFormatException: " + arr[1]);
				}
			cfgPrefixes.put(cfgPrefix, expectedTime);
		}

		/**
		 * Processes manually entered lines in the verify.txt
		 */
		private void process2(String line, Map<String, Integer> cfgPrefixes) {
			String[] arr = line.split(",");
			for (String s : arr) {
				cfgPrefixes.put(s.trim(), 0);
			}
		}

		/**
		 * Evaluates a configuration.
		 * 
		 * @param cfg
		 *            configuration that needs to be evaluated
		 * @param cfgName
		 *            name of the configuration. This is just for logging
		 *            purpose.
		 */
		private List<Long> evaluateConfig(Configuration cfg, String cfgName) {
			System.out.println("Evaluating " + cfgName);
			int count = Options.evaluationCount;
			List<Long> runningTime = new ArrayList<>(count);
			Pair<Boolean, Long> ret;
			if (cfg != null) {
				for (int i = 0; i < count; i++) {
					logger.newConfiguration(cfgName);
					ret = configurer.reconfigure(cfg, 0);
					if (ret.first) {
						prognosticator.time(ret.second);
						runningTime.add(ret.second);
					} else {
						System.err.println("Evaluation failed...");
					}
				}
			} else {
				System.err.println("Null configuration\n");
			}
			return runningTime;
		}
	}
}