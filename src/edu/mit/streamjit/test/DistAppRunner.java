package edu.mit.streamjit.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioBenchmarkProvider;

/**
 * @author sumanan
 * @since 25 Feb, 2015
 */
public final class DistAppRunner {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		int noOfNodes;
		Stopwatch sw = Stopwatch.createStarted();
		try {
			noOfNodes = Integer.parseInt(args[0]);
		} catch (Exception ex) {
			noOfNodes = 3;
		}

		// startSNs(noOfNodes);
		StreamCompiler compiler = new DistributedStreamCompiler(noOfNodes);

		Benchmark benchmark = new FMRadioBenchmarkProvider().iterator().next();
		Dataset dataset = benchmark.inputs().get(0);
		Input<Object> input = Datasets.cycle(dataset.input());

		OneToOneElement<Object, Object> streamGraph = benchmark.instantiate();
		CompiledStream stream = compiler.compile(streamGraph, input,
				Output.blackHole());
		stream.awaitDrained();

		sw.stop();
		long elapsedMills = sw.elapsed(TimeUnit.MILLISECONDS);
		String appName = streamGraph.getClass().getSimpleName();
		updateReadMeTxt(appName, benchmark.toString(), elapsedMills);
	}

	private static void startSNs(int noOfNodes) throws IOException {
		for (int i = 1; i < noOfNodes; i++)
			new ProcessBuilder("xterm", "-e", "java", "-jar", "StreamNode.jar")
					.start();
		// new ProcessBuilder("java", "-jar", "StreamNode.jar").start();
	}

	/**
	 * [25 Feb, 2015] TODO: This is a temporary fix to update the benchmark name
	 * ( that is more descriptive than plain appName) to the README.txt.
	 * Consider passing the benchmarkName to the
	 * {@link DistributedStreamCompiler} and let it to update the README.txt.
	 */
	private static void updateReadMeTxt(String appName, String benchmarkName,
			long elapsedMills) throws IOException {
		FileWriter writer = new FileWriter(String.format("%s%sREADME.txt",
				appName, File.separator), true);
		writer.write(String.format("benchmarkName=%s\n", benchmarkName));
		writer.write(String.format("TotalRunningTime=%s\n",
				elapsedTime(elapsedMills)));
		writer.close();
	}

	private static String elapsedTime(long mills) {
		String hms = String.format(
				"%02dH:%02dM:%02dS",
				TimeUnit.MILLISECONDS.toHours(mills),
				TimeUnit.MILLISECONDS.toMinutes(mills)
						% TimeUnit.HOURS.toMinutes(1),
				TimeUnit.MILLISECONDS.toSeconds(mills)
						% TimeUnit.MINUTES.toSeconds(1));
		return hms;
	}
}
