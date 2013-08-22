package edu.mit.streamjit.test.sanity;

import com.google.common.base.Supplier;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Datasets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

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
					simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), 7, 1, 1),
					simulateRoundrobin(Datasets.nCopies(100, "STRING"), 7, 1, 1));
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
					simulateRoundrobin(Datasets.allIntsInRange(0, 1_000_000), 7, 5, 5),
					simulateRoundrobin(Datasets.nCopies(100, "STRING"), 7, 5, 5));
		}
	}

	//TODO: a split 5 join 3 test (will actually reorder elements)

	/**
	 * Simulates a roundrobin splitjoin, returning a Dataset with reference
	 * output.
	 */
	private static Dataset simulateRoundrobin(Dataset dataset, int width, int splitRate, int joinRate) {
		int[] splitRates = new int[width], joinRates = new int[width];
		Arrays.fill(splitRates, splitRate);
		Arrays.fill(joinRates, joinRate);
		return simulateRoundrobin(dataset, width, splitRates, joinRates);
	}

	/**
	 * Simulates a weighted roundrobin splitjoin, returning a Dataset with
	 * reference output.
	 */
	private static Dataset simulateRoundrobin(Dataset dataset, int width, int[] splitRates, int[] joinRates) {
		List<Queue<Object>> bins = new ArrayList<>(width);
		for (int i = 0; i < width; ++i)
			bins.add(new ArrayDeque<>());

		int splitReq = 0;
		for (int i : splitRates)
			splitReq += i;

		Buffer buffer = InputBufferFactory.unwrap(dataset.input()).createReadableBuffer(splitReq);
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
		return Dataset.builder(dataset).output(Input.fromIterable(output)).build();
	}

	private static boolean ready(List<Queue<Object>> bins, int[] joinRates) {
		for (int i = 0; i < bins.size(); ++i)
			if (bins.get(i).size() < joinRates[i])
				return false;
		return true;
	}
}
