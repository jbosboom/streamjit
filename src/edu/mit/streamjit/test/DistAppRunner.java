package edu.mit.streamjit.test;

import java.io.IOException;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.apps.channelvocoder7.ChannelVocoder7;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioBenchmarkProvider;
import edu.mit.streamjit.test.sanity.nestedsplitjoinexample.NestedSplitJoin.NestedSplitJoinBenchmarkProvider;

public final class DistAppRunner {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		int noOfNodes;

		try {
			noOfNodes = Integer.parseInt(args[0]);
		} catch (Exception ex) {
			noOfNodes = 3;
		}

		Benchmark benchmark = new NestedSplitJoinBenchmarkProvider().iterator()
				.next();
		StreamCompiler compiler = new DistributedStreamCompiler(noOfNodes);

		Dataset dataset = benchmark.inputs().get(0);
		Input<Object> input = Datasets.cycle(dataset.input());

		CompiledStream stream = compiler.compile(benchmark.instantiate(),
				input, Output.blackHole());
		stream.awaitDrained();
	}

	private static void startSNs(int noOfNodes) throws IOException {
		for (int i = 1; i < noOfNodes; i++)
			new ProcessBuilder("xterm", "-e", "java", "-jar", "StreamNode.jar")
					.start();
		new ProcessBuilder("java", "-jar", "StreamNode.jar").start();
	}
}
