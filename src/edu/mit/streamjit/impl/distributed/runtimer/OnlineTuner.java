package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.tuner.OpenTuner;
import edu.mit.streamjit.tuner.TCPTuner;
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
		int tryCount = 0;
		try {
			tuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
					File.separator, File.separator));

			tuner.writeLine("program");
			tuner.writeLine(app.name);

			tuner.writeLine("confg");
			tuner.writeLine(Jsonifiers.toJson(app.blobConfiguration).toString());

			System.out.println("New tune run.............");
			while (manager.getStatus() != AppStatus.STOPPED) {
				String cfgJson = tuner.readLine();
				if (cfgJson == null)
					break;

				// At the end of the tuning, Opentuner will send "Completed"
				// msg. This means no more tuning.
				if (cfgJson.equals("Completed")) {
					handleTermination();
					break;
				}

				System.out
						.println("----------------------------------------------");
				System.out.println(tryCount++);

				Configuration config = Configuration.fromJson(cfgJson);

				if (GlobalConstants.saveAllConfigurations)
					saveConfg(cfgJson, tryCount);

				try {
					if (!cfgManager.newConfiguration(config)) {
						tuner.writeLine("-1");
						continue;
					}

					if (manager.isRunning()) {
						boolean state = drainer.startDraining(0);
						if (!state) {
							System.err
									.println("Final drain has already been called. no more tuning.");
							tuner.writeLine("exit");
							break;
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

					drainer.setBlobGraph(app.blobGraph);
					System.err.println("Reconfiguring...");
					if (manager.reconfigure()) {
						Stopwatch stopwatch = Stopwatch.createStarted();
						manager.awaitForFixInput();
						stopwatch.stop();
						// TODO: need to check the manager's status before
						// passing
						// the time. Exceptions, final drain, etc may causes app
						// to
						// stop executing.
						long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);

						System.out.println("Execution time is " + time
								+ " milli seconds");
						tuner.writeLine(new Double(time).toString());
					} else {
						tuner.writeLine("-1");
						continue;
					}
				} catch (Exception ex) {
					System.err
							.println("Couldn't compile the stream graph with this configuration");
					tuner.writeLine("-1");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
		if (needTermination) {
			if (manager.isRunning()) {
				drainer.startDraining(1);
			} else {
				manager.stop();
			}
		} else {
			runForever(finalConfg);
		}
	}

	/**
	 * TODO: Just copied from the run method. Code duplication between this
	 * method and the run() method. Try to avoid duplicate code.
	 * 
	 * @param cfgJson
	 */
	private void runForever(String cfgJson) {
		System.out.println("runForever");
		Configuration config = Configuration.fromJson(cfgJson);
		try {
			if (!cfgManager.newConfiguration(config)) {
				System.err.println("Invalid final configuration.");
				return;
			}

			if (manager.isRunning()) {
				boolean state = drainer.startDraining(0);
				if (!state) {
					System.err
							.println("Final drain has already been called. no more tuning.");
					return;
				}

				System.err.println("awaitDrainedIntrmdiate");
				drainer.awaitDrainedIntrmdiate();

				if (GlobalConstants.useDrainData) {
					System.err.println("awaitDrainData...");
					drainer.awaitDrainData();
					DrainData drainData = drainer.getDrainData();
					app.drainData = drainData;
				}

				drainer.setBlobGraph(app.blobGraph);
			}

			System.err.println("Reconfiguring...");
			boolean var = manager.reconfigure();
			if (var) {
				System.out
						.println("Application is running with the final configuration.");
			} else {
				System.err.println("Invalid final configuration.");
			}
		} catch (Exception ex) {
			System.err
					.println("Couldn't compile the stream graph with this configuration");
		}
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