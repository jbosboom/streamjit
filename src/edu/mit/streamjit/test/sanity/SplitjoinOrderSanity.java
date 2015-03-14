/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
 * TODO: the splitjoin simulator should be refactored into a splitter simulator
 * and joiner simulator, so we can plug any of the splitters with any of the
 * joiners.  Right now we have a copy for the duplicate splitter.
 * @see SplitjoinComputeSanity
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/20/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public final class SplitjoinOrderSanity implements BenchmarkProvider {
	@Override
	public Iterator<Benchmark> iterator() {
		Benchmark[] benchmarks = {
			rr_rr(1, 1, 1),
			rr_rr(1, 5, 5),
			rr_rr(1, 7, 5),
			rr_rr(1, 5, 7),
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

			dup_rr(1, 1),
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
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(RoundrobinSimulator.create(dataset.input(), width, splitRate, joinRate)));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new RoundrobinSplitterSupplier(splitRate), new RoundrobinJoinerSupplier(joinRate)),
				dataset);
	}

	private static Benchmark wrr_rr(int width, int[] splitRates, int joinRate) {
		String name = String.format("WRR(%s) x %dw x RR(%d)", Arrays.toString(splitRates), width, joinRate);
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(RoundrobinSimulator.create(dataset.input(), width, splitRates, joinRate)));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new WeightedRoundrobinSplitterSupplier(splitRates), new RoundrobinJoinerSupplier(joinRate)),
				dataset);
	}

	private static Benchmark rr_wrr(int width, int splitRate, int[] joinRates) {
		String name = String.format("RR(%d) x %dw x WRR(%s)", splitRate, width, Arrays.toString(joinRates));
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(RoundrobinSimulator.create(dataset.input(), width, splitRate, joinRates)));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new RoundrobinSplitterSupplier(splitRate), new WeightedRoundrobinJoinerSupplier(joinRates)),
				dataset);
	}

	private static Benchmark wrr_wrr(int width, int[] splitRates, int[] joinRates) {
		String name = String.format("WRR(%s) x %dw x WRR(%s)", Arrays.toString(splitRates), width, Arrays.toString(joinRates));
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(RoundrobinSimulator.create(dataset.input(), width, splitRates, joinRates)));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new WeightedRoundrobinSplitterSupplier(splitRates), new WeightedRoundrobinJoinerSupplier(joinRates)),
				dataset);
	}

	private static Benchmark dup_rr(int width, int joinRate) {
		String name = String.format("dup x %dw x RR(%d)", width, joinRate);
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(DuplicateSimulator.create(dataset.input(), width, joinRate)));
		return new SuppliedBenchmark(name,
				new SplitjoinSupplier(width, new DuplicateSplitterSupplier(), new RoundrobinJoinerSupplier(joinRate)),
				dataset);
	}

	private static Benchmark dup_wrr(int width, int[] joinRates) {
		String name = String.format("dup x %dw x WRR(%s)", width, Arrays.toString(joinRates));
		Dataset dataset = Datasets.allIntsInRange(0, 1_000_000);
		dataset = dataset.withOutput(Datasets.lazyInput(DuplicateSimulator.create(dataset.input(), width, joinRates)));
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

	private static final class RoundrobinSimulator<T> implements Supplier<Input<T>> {
		private final Input<T> input;
		private final int width;
		private final int[] splitRates, joinRates;
		public static <T> RoundrobinSimulator<T> create(Input<T> input, int width, int splitRate, int joinRate) {
			int[] splitRates = new int[width], joinRates = new int[width];
			Arrays.fill(splitRates, splitRate);
			Arrays.fill(joinRates, joinRate);
			return create(input, width, splitRates, joinRates);
		}
		public static <T> RoundrobinSimulator<T> create(Input<T> input, int width, int[] splitRates, int joinRate) {
			int[] joinRates = new int[width];
			Arrays.fill(joinRates, joinRate);
			return create(input, width, splitRates, joinRates);
		}
		public static <T> RoundrobinSimulator<T> create(Input<T> input, int width, int splitRate, int[] joinRates) {
			int[] splitRates = new int[width];
			Arrays.fill(splitRates, splitRate);
			return create(input, width, splitRates, joinRates);
		}
		public static <T> RoundrobinSimulator<T> create(Input<T> input, int width, int[] splitRates, int[] joinRates) {
			return new RoundrobinSimulator<>(input, width, splitRates, joinRates);
		}
		private RoundrobinSimulator(Input<T> input, int width, int[] splitRates, int[] joinRates) {
			this.input = input;
			this.width = width;
			this.splitRates = splitRates;
			this.joinRates = joinRates;
		}
		@Override
		@SuppressWarnings("unchecked")
		public Input<T> get() {
			List<Queue<Object>> bins = new ArrayList<>(width);
			for (int i = 0; i < width; ++i)
				bins.add(new ArrayDeque<>());

			int splitReq = 0;
			for (int i : splitRates)
				splitReq += i;

			Buffer buffer = InputBufferFactory.unwrap(input).createReadableBuffer(splitReq);
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
			return (Input<T>)Input.fromIterable(output);
		}
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
			ArrayDeque<T> bin = new ArrayDeque<>();
			Buffer buffer = InputBufferFactory.unwrap(input).createReadableBuffer(42);
			while (buffer.size() > 0) {
				Object o = buffer.read();
				bin.add((T)o);
			}

			List<Queue<T>> bins = new ArrayList<>(width);
			//Use the first one, make copies for the rest.
			bins.add(bin);
			while (bins.size() < width)
				bins.add(new ArrayDeque<>(bin));

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
