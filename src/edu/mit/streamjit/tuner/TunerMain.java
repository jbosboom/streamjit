package edu.mit.streamjit.tuner;

import java.io.File;
import java.io.IOException;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.apps.fmradio.FMRadio;
import edu.mit.streamjit.util.json.Jsonifiers;

import com.google.common.base.Splitter;

/**
 * 
 * Tuner path /home/sumanan/opentuner/opentuner/examples/streamjit/tuner.py
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 20, 2013
 */
public class TunerMain {

	AutoTuner autoTuner;

	public TunerMain() {
		autoTuner = new TCPTuner();
	}

	private String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}

	private double runApp(Benchmark app, Configuration cfg)
			throws InterruptedException {
		StreamCompiler sc;
		IntParameter CompilerType = cfg.getParameter("compiler",
				IntParameter.class);
		int val = CompilerType.getValue();
		if (val == 0)
			sc = new DebugStreamCompiler();
		else
			sc = new ConcurrentStreamCompiler(val);

		long startTime = System.nanoTime();

		Dataset dataset = app.inputs().get(0);

		Input<Object> input = dataset.input();
		Output<Object> output = Output.blackHole();

		run(sc, app.instantiate(), input, output);
		long endTime = System.nanoTime();
		double diff = (endTime - startTime) / 1e6;
		return diff;
	}

	public void tune(Benchmark app, Configuration cfg)
			throws InterruptedException {
		int tryCount = 1;
		try {
			autoTuner
					.startTuner("lib/opentuner/streamjit/streamjit.py");
			autoTuner.writeLine("program");
			autoTuner.writeLine("FMRadio");

			autoTuner.writeLine("confg");
			String s = getConfigurationString(cfg);
			autoTuner.writeLine(s);

			while (true) {
				String pythonDict = autoTuner.readLine();
				System.out
						.println("----------------------------------------------");
				System.out.println("Try Count = " + tryCount++);
				Configuration config = rebuildConfiguraion(pythonDict, cfg);
				double time = runApp(app, config);
				System.out.println("Execution time is " + time
						+ " milli seconds");
				autoTuner.writeLine(new Double(time).toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private Configuration rebuildConfiguraion(String pythonDict,
			Configuration config) {
		pythonDict = pythonDict.replaceAll("u'", "");
		pythonDict = pythonDict.replaceAll("':", "");
		pythonDict = pythonDict.replaceAll("\\{", "");
		pythonDict = pythonDict.replaceAll("\\}", "");
		Splitter dictSplitter = Splitter.on(", ").omitEmptyStrings()
				.trimResults();
		Configuration.Builder builder = Configuration.builder();
		System.out.println("New parameters...");
		for (String s : dictSplitter.split(pythonDict)) {
			String[] str = s.split(" ");
			if (str.length != 2)
				throw new AssertionError("Wrong python dictionary...");
			IntParameter ip = config.getParameter(str[0], IntParameter.class);
			checkNotNull(ip, String.format(
					"No parameter %s found in the configuraion", str[0]));

			builder.addParameter(new IntParameter(ip.getName(), ip.getMin(), ip
					.getMax(), Integer.parseInt(str[1])));
			System.out.println(String.format("\t%s = %s", str[0], str[1]));
		}
		return builder.build();
	}

	private void run(StreamCompiler compiler,
			OneToOneElement<Object, Object> streamGraph, Input<Object> input,
			Output<Object> output) {
		System.out.println("Running the StreamJit application...");
		CompiledStream stream = compiler.compile(streamGraph, input, output);
		try {
			stream.awaitDrained();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 *            [0] - String topLevelWorkerName args[1] - String jarFilePath
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {

		Benchmark fmBench = new FMRadio.FMRadioBenchmark();
		Configuration.Builder builder = Configuration.builder();
		IntParameter ip = new IntParameter("compiler", 0, 10, 8);
		builder.addParameter(ip);
		builder.addParameter(new IntParameter("multiplier", 1, 1000000, 10));
		Configuration cfg = builder.build();

		TunerMain tuner = new TunerMain();
		tuner.tune(fmBench, cfg);
	}
}
