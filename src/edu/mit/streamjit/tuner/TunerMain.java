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
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
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
		/*
		 * IntParameter compilerType = cfg.getParameter("compiler",
		 * IntParameter.class);
		 */

		IntParameter multiplier = cfg.getParameter("multiplier",
				IntParameter.class);
		// int val = compilerType.getValue();
		/*
		 * if (val == 0) sc = new DebugStreamCompiler(); else sc = new
		 * ConcurrentStreamCompiler(val);
		 */

		int mul = multiplier.getValue();
		sc = new CompilerStreamCompiler().multiplier(mul);

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
			autoTuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit.py", File.separator,
					File.separator, File.separator));
			// autoTuner.startTuner("/home/sumanan/opentuner/NewOT-30-8-13/opentuner/streamjit/streamjit.py");
			autoTuner.writeLine("program");
			autoTuner.writeLine("FMRadio");

			autoTuner.writeLine("confg");
			String s = getConfigurationString(cfg);
			autoTuner.writeLine(s);

			while (true) {
				String pythonDict = autoTuner.readLine();
				System.out
						.println("----------------------------------------------");
				System.out.println(tryCount++);
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
		// IntParameter ip = new IntParameter("compiler", 0, 10, 8);
		// builder.addParameter(ip);
		builder.addParameter(new IntParameter("multiplier", 1, 1000, 10));
		Configuration cfg = builder.build();

		TunerMain tuner = new TunerMain();
		tuner.tune(fmBench, cfg);
	}
}
