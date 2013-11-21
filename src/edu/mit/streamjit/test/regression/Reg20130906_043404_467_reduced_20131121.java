package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.common.CheckVisitor;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * @since 9/6/2013 4:34PM EDT
 */
@ServiceProvider(Benchmark.class)
public class Reg20130906_043404_467_reduced_20131121 implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(),
				new edu.mit.streamjit.impl.common.TestFilters.Batcher(2),
				new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(2)
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
		new Reg20130906_043404_467_reduced_20131121().instantiate().visit(new CheckVisitor());
		Benchmarker.runBenchmark(new Reg20130906_043404_467_reduced_20131121(), new edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler()).get(0).print(System.out);
	}
}

