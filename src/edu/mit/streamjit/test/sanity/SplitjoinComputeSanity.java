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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
			for (int i = 0; i < 100000; ++i) {
				input.add(i);
				output.add(i * FACTORS[i % FACTORS.length]);
			}
			return Dataset.builder().name("0 to 100000").input(Input.<Object>fromIterable(input)).output(Input.<Object>fromIterable(output)).build();
		}
	}
}
