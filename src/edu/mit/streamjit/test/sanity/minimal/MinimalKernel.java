/**
 * @author Sumanan sumanan@mit.edu
 * @since Jun 9, 2013
 */
package edu.mit.streamjit.test.sanity.minimal;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamPrinter;

public class MinimalKernel extends Pipeline<Integer, Void> {

	public MinimalKernel() {
		// super(new IntSource(1, 1, 0), new IntPrinter(1000, 0, 0));
		super(new IntSource(1, 1, 0), new StreamPrinter<>());
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
				// System.out.println(pop());
				pop();
		}
	}

}
