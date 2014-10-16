package edu.mit.streamjit.test.sanity;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Arrays;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/18/2013
 */
@ServiceProvider(Benchmark.class)
public class IdentitySanity extends SuppliedBenchmark {
	public IdentitySanity() {
		super("IdentitySanity", Identity.class,
				id(Datasets.allIntsInRange(0, 1_000_000)),
				id(Datasets.nCopies(100, "STRING")),
				id(new Dataset("foo", Input.fromIterable(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))))
				);
	}

	private static Dataset id(Dataset dataset) {
		return dataset.withOutput(dataset.input());
	}

	public static void main(String[] args) {
		for (Benchmarker.Result r : Benchmarker.runBenchmark(new IdentitySanity(), new Compiler2StreamCompiler()))
			r.print(System.out);
	}
}
