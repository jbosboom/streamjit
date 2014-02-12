package edu.mit.streamjit.test.regression;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/11/2014
 */
@ServiceProvider(Benchmark.class)
public class StaticFieldUsedInWork implements Benchmark {
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public OneToOneElement<Object, Object> instantiate() {
		return new Pipeline(new StaticFieldUser(0), new StaticFieldUser(1), new StaticFieldUser(2), new StaticFieldUser(3)
//			, new InheritedStaticFieldUser()
		);
	}
	@Override
	public List<Dataset> inputs() {
		Dataset ds = Datasets.allIntsInRange(0, 1000);
		return Collections.singletonList(ds.withOutput(Datasets.outputOf(new DebugStreamCompiler(), instantiate(), ds.input())));
	}
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	public static void main(String[] args) {
		Benchmarker.runBenchmark(new StaticFieldUsedInWork(), new Compiler2StreamCompiler()).get(0).print(System.out);
	}

	private static class StaticFieldUser extends Filter<Integer, Integer> {
		protected static final int[] x = {0, 1, 2, 3};
		private final int i;
		private StaticFieldUser(int i) {
			super(1, 1);
			this.i = i;
		}
		@Override
		public void work() {
			push(pop() + x[i]);
		}
	}

	private static final class InheritedStaticFieldUser extends StaticFieldUser {
		private InheritedStaticFieldUser() {
			super(0);
		}
		@Override
		public void work() {
			push(pop() + x[x.length-1]);
		}
	}
}
