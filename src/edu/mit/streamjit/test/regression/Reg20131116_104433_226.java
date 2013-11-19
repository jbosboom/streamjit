package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 * @since 11/16/2013 10:44PM EST
 */
@ServiceProvider(Benchmark.class)
public class Reg20131116_104433_226 implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Pipeline(new Identity<>(),
				new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter<>(), new edu.mit.streamjit.api.RoundrobinJoiner(), new OneToOneElement[]{
						new edu.mit.streamjit.impl.common.TestFilters.PeekingAdder(2),
						new Identity(),
						}));
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
		Benchmarker.runBenchmark(new Reg20131116_104433_226(), new edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler()).get(0).print(System.out);
	}
}

