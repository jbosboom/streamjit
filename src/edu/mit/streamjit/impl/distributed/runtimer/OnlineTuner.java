package edu.mit.streamjit.impl.distributed.runtimer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
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
	private final boolean needTermination;

	public OnlineTuner(AbstractDrainer drainer, StreamJitAppManager manager,
			StreamJitApp app, boolean needTermination) {
		this.drainer = drainer;
		this.manager = manager;
		this.app = app;
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
			String s = getConfigurationString(app.blobConfiguration);
			tuner.writeLine(s);

			System.out.println("New tune run.............");
			while (manager.getStatus() != AppStatus.STOPPED) {
				String pythonDict = tuner.readLine();
				if (pythonDict == null)
					break;

				if (pythonDict.equals("Completed")) {
					String finalConfg = tuner.readLine();
					System.out.println("Tuning finished");
					saveFinalConfg(finalConfg);
					if (needTermination) {
						if (manager.isRunning()) {
							drainer.startDraining(1);
							System.err.println("awaitDrainedIntrmdiate");
							try {
								drainer.awaitDrainedIntrmdiate();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						} else {
							manager.stop();
						}
					} else {
						runForever(finalConfg);
					}
					break;
				}

				System.out
						.println("----------------------------------------------");
				System.out.println(tryCount++);
				Configuration config = rebuildConfiguraion(pythonDict,
						app.blobConfiguration);
				try {
					if (!app.newConfiguration(config)) {
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

						// System.err.println("awaitDrainData...");
						drainer.awaitDrainData();
						DrainData drainData = drainer.getDrainData();

						app.drainData = drainData;
					}

					drainer.setBlobGraph(app.blobGraph);
					System.err.println("Reconfiguring...");
					if (manager.reconfigure()) {
						Stopwatch stopwatch = new Stopwatch();
						stopwatch.start();
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
	 * TODO: Just copied from the run method. Try to avoid duplicate code.
	 * 
	 * @param pythonDict
	 */
	private void runForever(String pythonDict) {
		System.out.println("runForever");
		Configuration config = rebuildConfiguraion(pythonDict,
				app.blobConfiguration);
		try {
			if (!app.newConfiguration(config)) {
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

				// System.err.println("awaitDrainData...");
				drainer.awaitDrainData();
				DrainData drainData = drainer.getDrainData();

				app.drainData = drainData;
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
	 * Creates a new {@link Configuration} from the received python dictionary
	 * string. This is not a good way to do.
	 * <p>
	 * TODO: Need to add a method to {@link Configuration} so that the
	 * configuration object can be updated from the python dict string. Now we
	 * are destructing the old confg object and recreating a new one every time.
	 * Not a appreciatable way.
	 * 
	 * @param pythonDict
	 *            Python dictionary string. Autotuner gives a dictionary of
	 *            features with trial values.
	 * @param config
	 *            Old configuration object.
	 * @return New configuration object with updated values from the pythonDict.
	 */
	private Configuration rebuildConfiguraion(String pythonDict,
			Configuration config) {
		// System.out.println(pythonDict);
		checkNotNull(pythonDict, "Received Python dictionary is null");
		pythonDict = pythonDict.replaceAll("u'", "");
		pythonDict = pythonDict.replaceAll("':", "");
		pythonDict = pythonDict.replaceAll("\\{", "");
		pythonDict = pythonDict.replaceAll("\\}", "");
		Splitter dictSplitter = Splitter.on(", ").omitEmptyStrings()
				.trimResults();
		Configuration.Builder builder = Configuration.builder();
		System.out.println("New parameter values from Opentuner...");
		for (String s : dictSplitter.split(pythonDict)) {
			String[] str = s.split(" ");
			if (str.length != 2)
				throw new AssertionError("Wrong python dictionary...");
			Parameter p = config.getParameter(str[0]);
			if (p == null)
				continue;
			// System.out.println(String.format("\t%s = %s", str[0], str[1]));
			if (p instanceof IntParameter) {
				IntParameter ip = (IntParameter) p;
				builder.addParameter(new IntParameter(ip.getName(),
						ip.getMin(), ip.getMax(), Integer.parseInt(str[1])));

			} else if (p instanceof SwitchParameter<?>) {
				SwitchParameter sp = (SwitchParameter) p;
				Class<?> type = sp.getGenericParameter();
				int val = Integer.parseInt(str[1]);
				SwitchParameter<?> sp1 = new SwitchParameter(sp.getName(),
						type, sp.getUniverse().get(val), sp.getUniverse());
				builder.addParameter(sp1);
			}

		}
		return builder.build();
	}

	private String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}

	/**
	 * @param finalCfg
	 *            : Python dictionary
	 */
	private void saveFinalConfg(String finalCfg) {
		Configuration config = rebuildConfiguraion(finalCfg,
				app.blobConfiguration);
		String json = config.toJson();
		try {
			FileWriter writer = new FileWriter(
					String.format("%s.cfg", app.name), false);
			writer.write(json);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}