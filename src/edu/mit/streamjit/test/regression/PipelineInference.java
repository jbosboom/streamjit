package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.common.TestFilters.Adder;
import edu.mit.streamjit.impl.common.TestFilters.ExtractMax;
import edu.mit.streamjit.impl.common.TestFilters.ListGatherer;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/30/2013
 */
@ServiceProvider(Benchmark.class)
public class PipelineInference implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Pipeline(
				new ListGatherer(5),
				new ExtractMax(),
				new Adder(0), //provide a root for inference
//				new Filter<Object, Object>(1,1) {
//					@Override
//					public void work() {
//						push(pop());
//					}
//				},
				new ListGatherer(5),
				new ExtractMax()
		);
	}
	@Override
	public List<Dataset> inputs() {
		Dataset ds = Datasets.allIntsInRange(0, 1000);
		return Collections.singletonList(ds.withOutput(Datasets.outputOf(new edu.mit.streamjit.impl.interp.InterpreterStreamCompiler(), instantiate(), ds.input())));
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	public static void main(String[] args) {
		Benchmarker.runBenchmark(new PipelineInference(), new edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler()).get(0).print(System.out);
	}
}

