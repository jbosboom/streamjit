package edu.mit.streamjit.test.sanity.weightedsplitjoinexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

public class WeightedSplitJoinExample {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		WeightedSplitJoinKernel kernel = new WeightedSplitJoinKernel();

		Input.ManualInput<Integer> input = Input.createManualInput();
		Output.ManualOutput<Void> output = Output.createManualOutput();

		StreamCompiler sc = new DebugStreamCompiler();
		CompiledStream stream = sc.compile(kernel, input, output);
		for (int i = 0; i < 10;) {
			if (input.offer(i)) {
				i++;
			} else
				Thread.sleep(10);
		}
		input.drain();
		while (stream.isDrained())
			;
	}

	private static class WeightedSplitJoinKernel extends
			Pipeline<Integer, Void> {
		WeightedSplitJoinKernel() {
			add(new SplitJoin1(), new IntPrinter());
		}
	}

	private static class SplitJoin1 extends Splitjoin<Integer, Integer> {
		SplitJoin1() {
			super(new RoundrobinSplitter<Integer>(),
					new WeightedRoundrobinJoiner<Integer>(2));
			add(new Identity(), new Identity());
		}
	}

	private static class Identity extends Filter<Integer, Integer> {

		public Identity() {
			super(1, 1);
		}

		@Override
		public void work() {
			push(pop());
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
