/**
 * @author Sumanan sumanan@mit.edu
 * @since Apr 16, 2013
 */
package edu.mit.streamjit.test.sanity.nestedsplitjoinexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * This is just a nested, inner split join stream structure that is to verify
 * the correctness of the StreamJit system.
 */
public class NestedSplitJoin {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Pipeline<Integer, Void> core = new Pipeline<Integer, Void>(
				new nestedSplitJoinCore(), new IntPrinter());

		Input.ManualInput<Integer> input = Input.createManualInput();
		Output.ManualOutput<Void> output = Output.createManualOutput();

		StreamCompiler sc = new DebugStreamCompiler();
		// StreamCompiler sc = new ConcurrentStreamCompiler(2);
		// StreamCompiler sc = new DistributedStreamCompiler(2);

		CompiledStream stream = sc.compile(core, input, output);
		Integer result;
		for (int i = 0; i < 1000;) {
			if (input.offer(i)) {
				// System.out.println("Offer success " + i);
				i++;
			} else {
				// System.out.println("Offer failed " + i);
				Thread.sleep(10);
			}
		}
		input.drain();
		while (!stream.isDrained())
			;
		System.out.println("I am exiting..");
	}

	private static class nestedSplitJoinCore extends
			Splitjoin<Integer, Integer> {

		public nestedSplitJoinCore() {
			super(new RoundrobinSplitter<Integer>(),
					new RoundrobinJoiner<Integer>());
			add(new splitjoin1());
			add(new Pipeline<Integer, Integer>(new multiplier(1),
					new splitjoin1(), new multiplier(1)));
			add(new multiplier(1));
			add(new splitjoin2());
		}
	}

	private static class multiplier extends Filter<Integer, Integer> {
		int fact;

		multiplier(int fact) {
			super(1, 1);
			this.fact = fact;
		}

		@Override
		public void work() {
			push(fact * pop());
		}

	}

	private static class splitjoin1 extends Splitjoin<Integer, Integer> {

		public splitjoin1() {
			super(new RoundrobinSplitter<Integer>(),
					new<Integer> RoundrobinJoiner());
			add(new splitjoin2());
			add(new multiplier(1));
			add(new splitjoin2());

		}
	}

	private static class splitjoin2 extends Splitjoin<Integer, Integer> {
		public splitjoin2() {
			super(new DuplicateSplitter<Integer>(),
					new RoundrobinJoiner<Integer>());
			add(new multiplier(1));
			add(new multiplier(1));
			add(new multiplier(1));
		}
	}

	private static class IntPrinter extends Filter<Integer, Void> {

		public IntPrinter() {
			super(1, 0);
		}

		@Override
		public void work() {
			System.out.println(pop());
		}
	}
}
