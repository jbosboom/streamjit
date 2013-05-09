/**
 * @author Sumanan sumanan@mit.edu
 * @since Mar 7, 2013
 */

package edu.mit.streamjit.apps.minimal;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.StreamPrinter;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

public class Minimal {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws InterruptedException {

		MinimalKernel kernel = new MinimalKernel();
		//StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new ConcurrentStreamCompiler(2);
		CompiledStream<Integer, Void> stream = sc.compile(kernel);
		Integer output;
		for (int i = 0; i < 1000000; ++i) {
			stream.offer(i);
			// System.out.println("Offered" + i);
			// while ((output = stream.poll()) != null)
			// System.out.println(output);
		}
		stream.drain();
		System.out.println("Drain called");
		stream.awaitDraining();
		System.out.println("awaitDraining finished, Exiting");
	}

	private static class MinimalKernel extends Pipeline<Integer, Void> {

		public MinimalKernel() {
			// super(new IntSource(1, 1, 0), new IntPrinter(1000, 0, 0));
			super(new IntSource(1, 1, 0), new StreamPrinter<>());
		}
	}

	private static class IntSource extends StatefulFilter<Integer, Integer> {

		public IntSource(int i, int j, int k) {
			super(i, j, k);
		}

		@Override
		public void work() {
			push(pop());
		}
	}

	private static class IntPrinter extends Filter<Integer, Void> {

		public IntPrinter(int i, int j, int k) {
			super(i, j, k);
		}

		@Override
		public void work() {
			for (int i = 0; i < this.getPopRates().get(0).avg(); i++)
				System.out.println(pop());
		}
	}
}
