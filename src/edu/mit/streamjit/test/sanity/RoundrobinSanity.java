package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Datasets;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
public class RoundrobinSanity {
	@ServiceProvider(Benchmark.class)
	public static final class RoundrobinReorderSanity extends AbstractBenchmark {
		public RoundrobinReorderSanity() {
			super("RoundrobinReorderSanity", new Supplier<Splitjoin<Integer, Integer>>() {
					@Override
					public Splitjoin<Integer, Integer> get() {
						return new Splitjoin<>(
								new RoundrobinSplitter<Integer>(),
								new RoundrobinJoiner<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>());
					}
				},
					id(Datasets.allIntsInRange(0, 1_000_000)),
					id(Datasets.nCopies(100, "STRING")));
		}
	}

	@ServiceProvider(Benchmark.class)
	public static final class RoundrobinReorder5Sanity extends AbstractBenchmark {
		public RoundrobinReorder5Sanity() {
			super("RoundrobinReorder5Sanity", new Supplier<Splitjoin<Integer, Integer>>() {
					@Override
					public Splitjoin<Integer, Integer> get() {
						return new Splitjoin<>(
								new RoundrobinSplitter<Integer>(5),
								new RoundrobinJoiner<Integer>(5),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>(),
								new Identity<Integer>());
					}
				},
					id(Datasets.allIntsInRange(0, 1_000_000)),
					id(Datasets.nCopies(100, "STRING")));
		}
	}

	//TODO: a split 5 join 3 test (will actually reorder elements)

	private static Benchmark.Dataset id(Benchmark.Dataset dataset) {
		return Benchmark.Dataset.builder(dataset).output(dataset.input()).build();
	}
}
