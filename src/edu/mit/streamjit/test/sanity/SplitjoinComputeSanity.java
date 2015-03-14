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
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.common.TestFilters;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests that the different branches of a splitjoin perform their computation on
 * the correct elements.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/23/2013
 */
public class SplitjoinComputeSanity {
	/**
	 * @author Sumanan sumanan@mit.edu
	 * @since Mar 11, 2013 (as RoundRobinSplitterExample)
	 */
	@ServiceProvider(Benchmark.class)
	public static final class MultiplyBenchmark extends SuppliedBenchmark {
		private static final int[] FACTORS = {1, 2, 3};
		public MultiplyBenchmark() {
			super("SplitjoinComputeMultiply", new Supplier<OneToOneElement<Integer, Integer>>() {
				@Override
				public OneToOneElement<Integer, Integer> get() {
					Splitjoin<Integer, Integer> s = new Splitjoin<>(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>());
					for (int i = 0; i < FACTORS.length; ++i)
						s.add(new TestFilters.Multiplier(FACTORS[i]));
					return s;
				}
			}, dataset());
		}
		private static Dataset dataset() {
			List<Integer> input = new ArrayList<>(), output = new ArrayList<>();
			for (int i = 0; i+2 < 100000; i += 3) {
				input.add(i);
				output.add(i * FACTORS[i % FACTORS.length]);
				input.add(i+1);
				output.add((i+1) * FACTORS[(i+1) % FACTORS.length]);
				input.add(i+2);
				output.add((i+2) * FACTORS[(i+2) % FACTORS.length]);
			}
			//TODO: use a functional Input here
			return new Dataset("0 to 100000", Input.<Object>fromIterable(input)).withOutput(Input.<Object>fromIterable(output));
		}
	}
}
