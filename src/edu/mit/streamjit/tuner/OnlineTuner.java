package edu.mit.streamjit.tuner;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
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
	private final OpenTuner tuner;
	private final StreamJitApp<?, ?> app;
	private final ConfigurationManager cfgManager;
	private final boolean needTermination;
	private final TimeLogger logger;
	private final ConfigurationPrognosticator prognosticator;
	private final MethodTimeLogger mLogger;
	private final Reconfigurer configurer;

	public OnlineTuner(Reconfigurer configurer, boolean needTermination) {
		this.configurer = configurer;
		this.app = configurer.app;
		this.cfgManager = configurer.cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
		this.logger = configurer.logger;
		this.prognosticator = configurer.prognosticator;
		this.mLogger = configurer.mLogger;
	}

	@Override
	public void run() {
		if (Options.tune == 1)
			tune();
		else
			System.err.println("Options.tune is not in tune mode.");
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
			while (configurer.manager.getStatus() != AppStatus.STOPPED) {
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
			configurer.terminate();
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

		new Verifier(configurer).verify();

		if (needTermination) {
			configurer.terminate();
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
			configurer.drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (needTermination)
			configurer.terminate();

		try {
			TimeLogProcessor.summarize(app.name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}