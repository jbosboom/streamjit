package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.InterpreterStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/26/2013
 */
@ServiceProvider(Benchmark.class)
public class ReverseBitReverseRegTest implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return (OneToOneElement)bitReverse(8);
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
		Benchmarker.runBenchmark(new ReverseBitReverseRegTest(), new Compiler2StreamCompiler()).get(0).print(System.out);
	}

	//See Bill's thesis, page 35, but with the splitter and joiner exchanged.
	private static OneToOneElement<Integer, Integer> bitReverse(int n) {
		if (n == 2)
			return new Identity<Integer>();
		else
			return new Splitjoin<>(new RoundrobinSplitter<Integer>(n/2), new RoundrobinJoiner<Integer>(1),
					bitReverse(n/2), bitReverse(n/2));
	}
}