package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.compiler.CompilerStreamCompiler;
import edu.mit.streamjit.impl.interp.InterpreterStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * Regression test I've entered by hand to serve as a template for
 * automatically generating them when the fuzzer finds a bug.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/4/2013
 */
@ServiceProvider(Benchmark.class)
public class Reg20130803 implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Pipeline(new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2), new edu.mit.streamjit.impl.common.TestFilters.Batcher(10), new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3), new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3)), new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3), new edu.mit.streamjit.api.Identity(), new edu.mit.streamjit.impl.common.TestFilters.Adder(20)))), new edu.mit.streamjit.impl.common.TestFilters.Adder(1), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2), new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Adder(1), new Pipeline(new edu.mit.streamjit.api.Identity(), new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2))), new edu.mit.streamjit.api.Identity()));
	}
	@Override
	public List<Dataset> inputs() {
		Dataset ds = Datasets.allIntsInRange(0, 1000);
		return Collections.singletonList(ds.withOutput(Datasets.outputOf(new InterpreterStreamCompiler(), instantiate(), ds.input())));
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	public static void main(String[] args) {
		Benchmarker.runBenchmark(new Reg20130803(), new CompilerStreamCompiler()).get(0).print(System.out);
	}
}
