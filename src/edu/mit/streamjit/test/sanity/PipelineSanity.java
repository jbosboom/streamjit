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
