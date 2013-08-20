package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Rate;

/**
 * Some filters for tests.  Some of these may graduate to the API package in a
 * more general form (e.g., the math filters need to generalize to other types).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
public final class TestFilters {
	public static final class Adder extends Filter<Integer, Integer> {
		private final int addend;
		public Adder(int addend) {
			super(1, 1);
			this.addend = addend;
		}
		@Override
		public void work() {
			push(pop() + addend);
		}
	}

	public static final class Multiplier extends Filter<Integer, Integer> {
		private final int multiplier;
		public Multiplier(int multiplier) {
			super(1, 1);
			this.multiplier = multiplier;
		}
		@Override
		public void work() {
			push(pop() * multiplier);
		}
	}

	//TODO: this can be intrinsified into index math in the compiler.
	public static class Permuter extends Filter<Integer, Integer> {
		private final int[] permutation;
		public Permuter(int inputSize, int outputSize, int[] permutation) {
			super(Rate.create(inputSize), Rate.create(outputSize), Rate.create(0, outputSize));
			this.permutation = permutation.clone();
			for (int i : permutation)
				assert i >= 0 && i < inputSize;
			assert permutation.length == outputSize;
		}
		@Override
		public void work() {
			for (int i : permutation)
				push(peek(i));
			for (int i = 0; i < permutation.length; ++i)
				pop();
		}
	}

	public static final class Batcher extends Permuter {
		public Batcher(int batchSize) {
			super(batchSize, batchSize, makeIdentityPermutation(batchSize));
		}
		private static int[] makeIdentityPermutation(int batchSize) {
			int[] retval = new int[batchSize];
			for (int i = 0; i < retval.length; ++i)
				retval[i] = i;
			return retval;
		}
	}
}
