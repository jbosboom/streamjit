package edu.mit.streamjit.impl.distributed.runtimer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
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
	AbstractDrainer drainer;
	Controller controller;
	OpenTuner tuner;
	StreamJitApp app;

	public OnlineTuner(AbstractDrainer drainer, Controller controller,
			StreamJitApp app) {
		this.drainer = drainer;
		this.controller = controller;
		this.app = app;
		this.tuner = new TCPTuner();
	}

	@Override
	public void run() {
		int tryCount = 0;
		try {
			tuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
					File.separator, File.separator));

			tuner.writeLine("program");
			tuner.writeLine(app.topLevelClass);

			tuner.writeLine("confg");
			String s = getConfigurationString(app.blobConfiguration);
			tuner.writeLine(s);

			System.out.println("New tune run.............");
			while (true) {
				String pythonDict = tuner.readLine();
				if (pythonDict.equals("Completed")) {
					String finalConfg = tuner.readLine();
					System.out.println("Tuning finished");
					drainer.startDraining(1);
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
					drainer.setBlobGraph(app.blobGraph1);

					System.err.println("Reconfiguring...");
					controller.reconfigure();

					Stopwatch stopwatch = new Stopwatch();
					stopwatch.start();
					controller.awaitForFixInput();
					stopwatch.stop();
					long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);

					System.out.println("Execution time is " + time
							+ " milli seconds");
					tuner.writeLine(new Double(time).toString());
				} catch (Exception ex) {
					System.err
							.println("Couldn't compile the stream graph with this configuration");
					tuner.writeLine("-1");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
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
}