package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Datasets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public final class RoundrobinSanity implements BenchmarkProvider {
	@Override
	public Iterator<Benchmark> iterator() {
		Benchmark[] benchmarks = {
			rr_rr(7, 1, 1),
			rr_rr(7, 5, 5),
			rr_rr(7, 5, 3),
			rr_rr(7, 3, 5),
		};
		return Arrays.asList(benchmarks).iterator();
	}

	private static Benchmark rr_rr(int width, int splitRate, int joinRate) {
		String name = String.format("RR(%d) x %dw x RR(%d)", splitRate, width, joinRate);
		return new AbstractBenchmark(name,
				new SplitjoinSupplier(width, new RoundrobinSplitterSupplier(splitRate), new RoundrobinJoinerSupplier(joinRate)),
				simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), width, splitRate, joinRate));
	}

	private static final class SplitjoinSupplier implements Supplier<Splitjoin<Integer, Integer>> {
		private final int width;
		private final Supplier<? extends Splitter<Integer, Integer>> splitter;
		private final Supplier<? extends Joiner<Integer, Integer>> joiner;
		private SplitjoinSupplier(int width, Supplier<? extends Splitter<Integer, Integer>> splitter, Supplier<? extends Joiner<Integer, Integer>> joiner) {
			this.width = width;
			this.splitter = splitter;
			this.joiner = joiner;
		}
		@Override
		public Splitjoin<Integer, Integer> get() {
			ImmutableList.Builder<Identity<Integer>> builder = ImmutableList.builder();
			//Can't use Collections.nCopies because we need distinct filters.
			for (int i = 0; i < width; ++i)
				builder.add(new Identity<Integer>());
			return new Splitjoin<>(splitter.get(), joiner.get(), builder.build());
		}
	}

	//I'd like to use ConstructorSupplier here, but the generics won't work
	//because e.g. RoundrobinSplitter.class is a raw type.
	private static final class RoundrobinSplitterSupplier implements Supplier<Splitter<Integer, Integer>> {
		private final int rate;
		private RoundrobinSplitterSupplier(int rate) {
			this.rate = rate;
		}
		@Override
		public Splitter<Integer, Integer> get() {
			return new RoundrobinSplitter<>(rate);
		}
	}

	private static final class RoundrobinJoinerSupplier implements Supplier<Joiner<Integer, Integer>> {
		private final int rate;
		private RoundrobinJoinerSupplier(int rate) {
			this.rate = rate;
		}
		@Override
		public Joiner<Integer, Integer> get() {
			return new RoundrobinJoiner<>(rate);
		}
	}

	/**
	 * Simulates a roundrobin splitjoin, returning a Dataset with reference
	 * output.
	 */
	private static Dataset simulateRoundrobin(Dataset dataset, int width, int splitRate, int joinRate) {
		int[] splitRates = new int[width], joinRates = new int[width];
		Arrays.fill(splitRates, splitRate);
		Arrays.fill(joinRates, joinRate);
		return simulateRoundrobin(dataset, width, splitRates, joinRates);
	}

	/**
	 * Simulates a weighted roundrobin splitjoin, returning a Dataset with
	 * reference output.
	 */
	private static Dataset simulateRoundrobin(Dataset dataset, int width, int[] splitRates, int[] joinRates) {
		List<Queue<Object>> bins = new ArrayList<>(width);
		for (int i = 0; i < width; ++i)
			bins.add(new ArrayDeque<>());

		int splitReq = 0;
		for (int i : splitRates)
			splitReq += i;

		Buffer buffer = InputBufferFactory.unwrap(dataset.input()).createReadableBuffer(splitReq);
		while (buffer.size() >= splitReq)
			for (int i = 0; i < bins.size(); ++i)
				for (int j = 0; j < splitRates[i]; ++j)
					bins.get(i).add(buffer.read());

		List<Object> output = new ArrayList<>();
		while (ready(bins, joinRates)) {
			for (int i = 0; i < bins.size(); ++i)
				for (int j = 0; j < joinRates[i]; ++j)
					output.add(bins.get(i).remove());
		}
		return Dataset.builder(dataset).output(Input.fromIterable(output)).build();
	}

	private static boolean ready(List<Queue<Object>> bins, int[] joinRates) {
		for (int i = 0; i < bins.size(); ++i)
			if (bins.get(i).size() < joinRates[i])
				return false;
		return true;
	}
}
