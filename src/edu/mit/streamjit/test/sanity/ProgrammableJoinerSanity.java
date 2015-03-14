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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/12/2014
 */
@ServiceProvider(BenchmarkProvider.class)
public final class ProgrammableJoinerSanity implements BenchmarkProvider {
	public static void main(String[] args) {
		StreamCompiler sc = new Compiler2StreamCompiler();
		for (Benchmarker.Result r : Benchmarker.runBenchmarks(new ProgrammableJoinerSanity(), sc))
			r.print(System.out);
	}

	private static final int INPUTS = 10000;
	private static final Iterable<Integer> INPUT_ITERABLE = ContiguousSet.create(Range.closedOpen(0, INPUTS), DiscreteDomain.integers());
	@Override
	public Iterator<Benchmark> iterator() {
		return ImmutableList.of(
				create(1),
				create(2),
				create(3),
				create(10)
		).iterator();
	}

	private static Benchmark create(final int width) {
		Iterable<Integer> output = new Iterable<Integer>() {
			@Override
			public Iterator<Integer> iterator() {
				return new AbstractIterator<Integer>() {
					private final Iterator<Integer> input = INPUT_ITERABLE.iterator();
					@Override
					protected Integer computeNext() {
						if (!input.hasNext())
							return endOfData();
						int x = input.next();
						for (int i = 1; i < width; ++i)
							if (!input.hasNext())
								return endOfData();
							else
								x -= input.next();
						return x;
					}
				};
			}
		};
		Dataset ds = new Dataset(INPUT_ITERABLE.toString(), (Input)Input.fromIterable(INPUT_ITERABLE), (Supplier)Suppliers.ofInstance(Input.fromIterable(output)));
		return new AbstractBenchmark(""+width, ds) {
			@Override
			public OneToOneElement<Object, Object> instantiate() {
				Splitjoin<Integer, Integer> sj = new Splitjoin<>(new RoundrobinSplitter<Integer>(), new SubtractJoiner(width));
				for (int i = 0; i < width; ++i)
					sj.add(new Identity<>());
				return new Pipeline(
					new Identity<Integer>(),
					sj,
					new Identity<Integer>());
			}
		};
	}

	private static final class SubtractJoiner extends Joiner<Integer, Integer> {
		private final int width;
		private SubtractJoiner(int width) {
			this.width = width;
		}
		@Override
		public int supportedInputs() {
			return width;
		}
		@Override
		public void work() {
			int x = pop(0);
			for (int i = 1; i < width; ++i)
				x -= pop(i);
			push(x);
		}
		@Override
		public List<Rate> getPeekRates() {
			return Collections.nCopies(width, Rate.create(0));
		}
		@Override
		public List<Rate> getPopRates() {
			return Collections.nCopies(width, Rate.create(1));
		}
		@Override
		public List<Rate> getPushRates() {
			return ImmutableList.of(Rate.create(1));
		}
	}
}
