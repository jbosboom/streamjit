package edu.mit.streamjit.tuner;

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

	private String makeConfiguration() {
		Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("compiler", 0, 1, 0));
		Configuration cfg = builder.build();
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
			sc = new ConcurrentStreamCompiler(3);

		long startTime = System.nanoTime();

		Dataset dataset = app.inputs().get(0);

		Input<Object> input = dataset.input();
		Output<Object> output = Output.blackHole();

		run(sc, app.instantiate(), input, output);
		long endTime = System.nanoTime();

		double diff = (endTime - startTime) / 1e6;
		System.out.println("Time taken = " + diff + " milli seconds");
		return diff;
	}

	private void run(StreamCompiler compiler,
			OneToOneElement<Object, Object> streamGraph, Input<Object> input,
			Output<Object> output) {
		CompiledStream stream = compiler.compile(streamGraph, input, output);
		try {
			stream.awaitDrained();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 *            [0] - String topLevelWorkerName args[1] - String jarFilePath
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Benchmark fmBench = new FMRadio.FMRadioBenchmark();
		Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("compiler", 0, 10, 2));
		Configuration cfg = builder.build();

		TunerMain tuner = new TunerMain();
		tuner.runApp(fmBench, cfg);
	}
}
