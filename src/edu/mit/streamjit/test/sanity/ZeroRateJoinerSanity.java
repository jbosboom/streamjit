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
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/12/2014
 */
@ServiceProvider(BenchmarkProvider.class)
public final class ZeroRateJoinerSanity implements BenchmarkProvider {
	public static void main(String[] args) {
		StreamCompiler sc = new Compiler2StreamCompiler();
		for (Benchmarker.Result r : Benchmarker.runBenchmarks(new ZeroRateJoinerSanity(), sc))
			r.print(System.out);
	}

	private static final int INPUTS = 1000;
	private static final Iterable<Integer> INPUT_ITERABLE = ContiguousSet.create(Range.closedOpen(0, INPUTS), DiscreteDomain.integers());
	@Override
	public Iterator<Benchmark> iterator() {
		Benchmark[] b = {
			create(0, 1),
			create(1, 0),
			create(1, 0, 1),
			create(0, 1, 0),
		};
		return Arrays.asList(b).iterator();
	}

	private static Benchmark create(final int... distribution) {
		Iterable<Integer> output = new Iterable<Integer>() {
			@Override
			public Iterator<Integer> iterator() {
				return Iterators.<Integer>concat(new AbstractIterator<Iterator<Integer>>() {
					private final Iterator<Integer> input = INPUT_ITERABLE.iterator();
					@Override
					protected Iterator<Integer> computeNext() {
						List<Integer> x = new ArrayList<>(distribution.length);
						for (int i : distribution) {
							if (!input.hasNext())
								return endOfData();
							Integer a = input.next();
							if (i > 0)
								x.add(a);
						}
						return x.iterator();
					}
				});
			}
		};
		Dataset dataset = new Dataset(INPUT_ITERABLE.toString(), (Input)Input.fromIterable(INPUT_ITERABLE),
				(Supplier)Suppliers.ofInstance(Input.fromIterable(output)));

		return new AbstractBenchmark(Arrays.toString(distribution), dataset) {
			@Override
			@SuppressWarnings("unchecked")
			public OneToOneElement<Object, Object> instantiate() {
				Splitjoin<Integer, Integer> sj = new Splitjoin<>(new RoundrobinSplitter<Integer>(), new WeightedRoundrobinJoiner<Integer>(distribution));
				for (int i : distribution)
					sj.add(i == 0 ? new IntSink() : new Identity<>());
				return new Pipeline(new Identity<>(), sj, new Identity<>());
			}
		};
	}

	private static final class IntSink extends Filter<Integer, Integer> {
		private IntSink() {
			super(1, 0);
		}
		@Override
		public void work() {
			pop();
		}
	}
}
