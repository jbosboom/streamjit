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
package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.InterpreterStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/26/2013
 */
@ServiceProvider(BenchmarkProvider.class)
public class BitReverseRegTest implements BenchmarkProvider {
	private static final StreamCompiler REFERENCE = new InterpreterStreamCompiler();

	@Override
	public Iterator<Benchmark> iterator() {
		return Arrays.asList(new Benchmark[]{
			new BitReverse8RegTest(),
			new BitReverse32RegTest(),
			new DoubleBitReverse8RegTest(),
			new DoubleBitReverse32RegTest(),
			new DoubleBitReverse8To32RegTest(),
			new DoubleBitReverse32To8RegTest(),
			new ReverseBitReverse8RegTest(),
			new ReverseBitReverse32RegTest(),
			new DoubleReverseBitReverse8RegTest(),
			new DoubleReverseBitReverse32RegTest(),
			new DoubleReverseBitReverse8To32RegTest(),
			new DoubleReverseBitReverse32To8RegTest(),
			new RegularToReverseBitReverse8RegTest(),
			new RegularToReverseBitReverse32RegTest(),
			new RegularToReverseBitReverse8To32RegTest(),
			new RegularToReverseBitReverse32To8RegTest(),
			new ReverseToRegularBitReverse8RegTest(),
			new ReverseToRegularBitReverse32RegTest(),
			new ReverseToRegularBitReverse8To32RegTest(),
			new ReverseToRegularBitReverse32To8RegTest(),
		}).iterator();
	}

	private static final class BitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return (OneToOneElement)bitReverse(8);
		}
	}

	private static final class BitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return (OneToOneElement)bitReverse(32);
		}
	}

	private static final class DoubleBitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(8), (OneToOneElement)bitReverse(8));
		}
	}

	private static final class DoubleBitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(32), (OneToOneElement)bitReverse(32));
		}
	}

	private static final class DoubleBitReverse8To32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(8), (OneToOneElement)bitReverse(32));
		}
	}

	private static final class DoubleBitReverse32To8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(32), (OneToOneElement)bitReverse(8));
		}
	}

	private static final class ReverseBitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return (OneToOneElement)reverseBitReverse(8);
		}
	}

	private static final class ReverseBitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return (OneToOneElement)reverseBitReverse(32);
		}
	}

	private static final class DoubleReverseBitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(8), (OneToOneElement)reverseBitReverse(8));
		}
	}

	private static final class DoubleReverseBitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(32), (OneToOneElement)reverseBitReverse(32));
		}
	}

	private static final class DoubleReverseBitReverse8To32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(8), (OneToOneElement)reverseBitReverse(32));
		}
	}

	private static final class DoubleReverseBitReverse32To8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(32), (OneToOneElement)reverseBitReverse(8));
		}
	}

	private static final class RegularToReverseBitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(8), (OneToOneElement)reverseBitReverse(8));
		}
	}

	private static final class RegularToReverseBitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(32), (OneToOneElement)reverseBitReverse(32));
		}
	}

	private static final class RegularToReverseBitReverse8To32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(8), (OneToOneElement)reverseBitReverse(32));
		}
	}

	private static final class RegularToReverseBitReverse32To8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)bitReverse(32), (OneToOneElement)reverseBitReverse(8));
		}
	}

	private static final class ReverseToRegularBitReverse8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(8), (OneToOneElement)bitReverse(8));
		}
	}

	private static final class ReverseToRegularBitReverse32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(32), (OneToOneElement)bitReverse(32));
		}
	}

	private static final class ReverseToRegularBitReverse8To32RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(8), (OneToOneElement)bitReverse(32));
		}
	}

	private static final class ReverseToRegularBitReverse32To8RegTest extends BitReverseBenchmark {
		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline((OneToOneElement)reverseBitReverse(32), (OneToOneElement)bitReverse(8));
		}
	}

	//TODO: versions with Identity replaced with PeekingAdder
	//TODO: DuplicateSplitter?  probably just the size 8 one.

	//See Bill's thesis, page 35.
	private static OneToOneElement<Integer, Integer> bitReverse(int n) {
		if (n == 2)
			return new Identity<Integer>();
		else
			return new Splitjoin<>(new RoundrobinSplitter<Integer>(1), new RoundrobinJoiner<Integer>(n/2),
					bitReverse(n/2), bitReverse(n/2));
	}

	private static OneToOneElement<Integer, Integer> reverseBitReverse(int n) {
		if (n == 2)
			return new Identity<Integer>();
		else
			return new Splitjoin<>(new RoundrobinSplitter<Integer>(n/2), new RoundrobinJoiner<Integer>(1),
					bitReverse(n/2), bitReverse(n/2));
	}

	private static abstract class BitReverseBenchmark implements Benchmark {
		@Override
		public List<Dataset> inputs() {
			Dataset ds = Datasets.allIntsInRange(0, 1000);
			return Collections.singletonList(ds.withOutput(Datasets.outputOf(REFERENCE, instantiate(), ds.input())));
		}
		@Override
		public String toString() {
			return getClass().getSimpleName();
		}
	}

	public static void main(String[] args) {
		Benchmark benchmark = new BitReverse32RegTest();
		StreamCompiler compiler = new Compiler2StreamCompiler();
		Benchmarker.runBenchmark(benchmark, compiler).get(0).print(System.out);
	}
}