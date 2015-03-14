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
import edu.mit.streamjit.impl.common.TestFilters;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/27/2013
 */
public class PipelineSanity {
	@ServiceProvider(Benchmark.class)
	public static final class Add15 extends AbstractBenchmark {
		public Add15() {
			super("Add15", add15(Datasets.allIntsInRange(0, 100000)));
		}
		@Override
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline<>(new TestFilters.Adder(1),
					new TestFilters.Adder(2),
					new TestFilters.Adder(3),
					new TestFilters.Adder(4),
					new TestFilters.Adder(5));
		}
		@SuppressWarnings("unchecked")
		private static Dataset add15(Dataset dataset) {
			return dataset.withOutput(Datasets.transformOne(new Function<Integer, Integer>() {
				@Override
				public Integer apply(Integer input) {
					return input+15;
				}
			}, (Input)dataset.input()));
		}
	}

	/**
	 * This test's filters do not commute, so this tests filter ordering.
	 */
	@ServiceProvider(Benchmark.class)
	public static final class Plus3Times2Plus7 extends AbstractBenchmark {
		public Plus3Times2Plus7() {
			super("Plus3Times2Plus7", plus3Times2Plus7(Datasets.allIntsInRange(0, 100000)));
		}
		@Override
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline<>(new TestFilters.Adder(3),
					new TestFilters.Multiplier(2),
					new TestFilters.Adder(7));
		}
		@SuppressWarnings("unchecked")
		private static Dataset plus3Times2Plus7(Dataset dataset) {
			return dataset.withOutput(Datasets.transformOne(new Function<Integer, Integer>() {
				@Override
				public Integer apply(Integer input) {
					return (input+3)*2+7;
				}
			}, (Input)dataset.input()));
		}
	}

	public static void main(String[] args) {
		Benchmarker.runBenchmark(new Plus3Times2Plus7(), new CompilerStreamCompiler());
	}
}
