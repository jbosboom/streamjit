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

import com.google.common.base.Function;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.impl.common.TestFilters.StatefulAdder;
import edu.mit.streamjit.impl.common.TestFilters.StatefulMultiplier;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;

/**
 * Tests stateful filters.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/29/2013
 */
public class StatefulSanity {
	/**
	 * This test's filters do not commute, so this tests filter ordering.
	 * <p/>
	 * This is from PipelineSanity, just with stateful filters.
	 */
	@ServiceProvider(Benchmark.class)
	public static final class Plus3Times2Plus7 extends AbstractBenchmark {
		public Plus3Times2Plus7() {
			super("Stateful Plus3Times2Plus7", plus3Times2Plus7(Datasets.allIntsInRange(0, 100000)));
		}
		@Override
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline<>(new StatefulAdder(3),
					new StatefulMultiplier(2),
					new StatefulAdder(7));
		}
		@SuppressWarnings("unchecked")
		private static Dataset plus3Times2Plus7(Dataset dataset) {
			return dataset.withOutput(Datasets.transformOne(new Function<Integer, Integer>() {
				private int a1 = 3, m = 2, a2 = 7;
				@Override
				public Integer apply(Integer input) {
					int r = (input+a1)*m+a2;
					++a1;
					++m;
					++a2;
					return r;
				}
			}, (Input)dataset.input()));
		}
	}

	public static void main(String[] args) {
		Benchmarker.runBenchmark(new Plus3Times2Plus7(), new CompilerStreamCompiler()).get(0).print(System.out);
	}
}
