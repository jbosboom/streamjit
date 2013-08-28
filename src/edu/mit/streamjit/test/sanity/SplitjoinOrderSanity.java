package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.test.SuppliedBenchmark;
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
 * Tests that splitjoins using the built-in splitters order their elements
 * properly.
 *
 * TODO: we run the simulator even if we aren't going to run any of the
 * benchmarks.  We'll need to do that lazily at some point.
 *
 * TODO: the splitjoin simulator should be refactored into a splitter simulator
 * and joiner simulator, so we can plug any of the splitters with any of the
 * joiners.  Right now we have a copy for the duplicate splitter.
 * @see SplitjoinComputeSanity
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public final class SplitjoinOrderSanity implements BenchmarkProvider {
	@Override
	public Iterator<Benchmark> iterator() {
		Benchmark[] benchmarks = {
			rr_rr(7, 1, 1),
			rr_rr(7, 5, 5),
			rr_rr(7, 5, 3),
			rr_rr(7, 3, 5),

			wrr_rr(1, new int[]{1}, 1),
			wrr_rr(7, new int[]{1, 1, 1, 1, 1, 1, 1}, 1),
			wrr_rr(7, new int[]{5, 5, 5, 5, 5, 5, 5}, 5),

			rr_wrr(1, 1, new int[]{1}),
			rr_wrr(7, 1, new int[]{1, 1, 1, 1, 1, 1, 1}),
			rr_wrr(7, 5, new int[]{5, 5, 5, 5, 5, 5, 5}),

			wrr_wrr(1, new int[]{1}, new int[]{1}),
			wrr_wrr(7, new int[]{1, 1, 1, 1, 1, 1, 1}, new int[]{1, 1, 1, 1, 1, 1, 1}),
			wrr_wrr(7, new int[]{5, 5, 5, 5, 5, 5, 5}, new int[]{5, 5, 5, 5, 5, 5, 5}),
			wrr_wrr(7, new int[]{1, 2, 3, 4, 3, 2, 1}, new int[]{1, 2, 3, 4, 3, 2, 1}),

			dup_rr(7, 1),
			dup_rr(7, 7),

			dup_wrr(1, new int[]{1}),
			dup_wrr(7, new int[]{1, 1, 1, 1, 1, 1, 1}),
			dup_wrr(7, new int[]{5, 5, 5, 5, 5, 5, 5}),
		};
		return Arrays.asList(benchmarks).iterator();
	}

	private static Benchmark rr_rr(int width, int splitRate, int joinRate) {
		String name = String.format("RR(%d) x %dw x RR(%d)", splitRate, width, joinRate);
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new RoundrobinSplitterSupplier(splitRate), new RoundrobinJoinerSupplier(joinRate)),
				simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), width, splitRate, joinRate));
	}

	private static Benchmark wrr_rr(int width, int[] splitRates, int joinRate) {
		String name = String.format("WRR(%s) x %dw x RR(%d)", Arrays.toString(splitRates), width, joinRate);
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new WeightedRoundrobinSplitterSupplier(splitRates), new RoundrobinJoinerSupplier(joinRate)),
				simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), width, splitRates, joinRate));
	}

	private static Benchmark rr_wrr(int width, int splitRate, int[] joinRates) {
		String name = String.format("RR(%d) x %dw x WRR(%s)", splitRate, width, Arrays.toString(joinRates));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new RoundrobinSplitterSupplier(splitRate), new WeightedRoundrobinJoinerSupplier(joinRates)),
				simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), width, splitRate, joinRates));
	}

	private static Benchmark wrr_wrr(int width, int[] splitRates, int[] joinRates) {
		String name = String.format("WRR(%s) x %dw x WRR(%s)", Arrays.toString(splitRates), width, Arrays.toString(joinRates));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new WeightedRoundrobinSplitterSupplier(splitRates), new WeightedRoundrobinJoinerSupplier(joinRates)),
				simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), width, splitRates, joinRates));
	}

	private static Benchmark dup_rr(int width, int joinRate) {
		String name = String.format("dup x %dw x RR(%d)", width, joinRate);
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withInput(DuplicateSimulator.create(dataset.input(), width, joinRate).get());
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new DuplicateSplitterSupplier(), new RoundrobinJoinerSupplier(joinRate)),
				dataset);
	}

	private static Benchmark dup_wrr(int width, int[] joinRates) {
		String name = String.format("dup x %dw x WRR(%s)", width, Arrays.toString(joinRates));
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withInput(DuplicateSimulator.create(dataset.input(), width, joinRates).get());
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new DuplicateSplitterSupplier(), new WeightedRoundrobinJoinerSupplier(joinRates)),
				dataset);
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

	private static final class WeightedRoundrobinSplitterSupplier implements Supplier<Splitter<Integer, Integer>> {
		private final int[] rates;
		private WeightedRoundrobinSplitterSupplier(int[] rates) {
			this.rates = rates;
		}
		@Override
		public Splitter<Integer, Integer> get() {
			return new WeightedRoundrobinSplitter<>(rates);
		}
	}

	private static final class WeightedRoundrobinJoinerSupplier implements Supplier<Joiner<Integer, Integer>> {
		private final int[] rates;
		private WeightedRoundrobinJoinerSupplier(int[] rates) {
			this.rates = rates;
		}
		@Override
		public Joiner<Integer, Integer> get() {
			return new WeightedRoundrobinJoiner<>(rates);
		}
	}

	private static final class DuplicateSplitterSupplier implements Supplier<Splitter<Integer, Integer>> {
		@Override
		public Splitter<Integer, Integer> get() {
			return new DuplicateSplitter<>();
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

	private static Dataset simulateRoundrobin(Dataset dataset, int width, int[] splitRates, int joinRate) {
		int[] joinRates = new int[width];
		Arrays.fill(joinRates, joinRate);
		return simulateRoundrobin(dataset, width, splitRates, joinRates);
	}

	private static Dataset simulateRoundrobin(Dataset dataset, int width, int splitRate, int[] joinRates) {
		int[] splitRates = new int[width];
		Arrays.fill(splitRates, splitRate);
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
		return dataset.withOutput(Input.fromIterable(output));
	}

	private static final class DuplicateSimulator<T> implements Supplier<Input<T>> {
		private final Input<T> input;
		private final int width;
		private final int[] joinRates;
		public static <T> DuplicateSimulator<T> create(Input<T> input, int width, int joinRate) {
			int[] joinRates = new int[width];
			Arrays.fill(joinRates, joinRate);
			return create(input, width, joinRates);
		}
		public static <T> DuplicateSimulator<T> create(Input<T> input, int width, int[] joinRates) {
			return new DuplicateSimulator<>(input, width, joinRates);
		}
		private DuplicateSimulator(Input<T> input, int width, int[] joinRates) {
			this.input = input;
			this.width = width;
			this.joinRates = joinRates;
		}
		@Override
		@SuppressWarnings("unchecked")
		public Input<T> get() {
			List<Queue<T>> bins = new ArrayList<>(width);
			for (int i = 0; i < width; ++i)
				bins.add(new ArrayDeque<T>());

			Buffer buffer = InputBufferFactory.unwrap(input).createReadableBuffer(42);
			while (buffer.size() > 0) {
				Object o = buffer.read();
				for (int i = 0; i < bins.size(); ++i)
					bins.get(i).add((T)o);
			}

			List<T> output = new ArrayList<>();
			while (ready(bins, joinRates)) {
				for (int i = 0; i < bins.size(); ++i)
					for (int j = 0; j < joinRates[i]; ++j)
						output.add(bins.get(i).remove());
			}
			return Input.fromIterable(output);
		}
	}

	private static <T> boolean ready(List<Queue<T>> bins, int[] joinRates) {
		for (int i = 0; i < bins.size(); ++i)
			if (bins.get(i).size() < joinRates[i])
				return false;
		return true;
	}
}
