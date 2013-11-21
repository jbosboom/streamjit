package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Rate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		@Override
		public String toString() {
			return String.format("Adder(%d)", addend);
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
		@Override
		public String toString() {
			return String.format("Multiplier(%d)", multiplier);
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
		@Override
		public String toString() {
			return String.format("Permuter(%s)", Arrays.toString(permutation));
		}
	}

	public static final class Batcher extends Permuter {
		private final int batchSize;
		public Batcher(int batchSize) {
			super(batchSize, batchSize, makeIdentityPermutation(batchSize));
			this.batchSize = batchSize;
		}
		private static int[] makeIdentityPermutation(int batchSize) {
			int[] retval = new int[batchSize];
			for (int i = 0; i < retval.length; ++i)
				retval[i] = i;
			return retval;
		}
		@Override
		public String toString() {
			return String.format("Batcher(%d)", batchSize);
		}
	}

	public static final class ArrayHasher extends Filter<Object, Integer> {
		private final int n;
		public ArrayHasher(int n) {
			super(n, 1);
			this.n = n;
		}
		@Override
		public void work() {
			Object[] obj = new Object[n];
			for (int i = 0; i < n; ++i)
				obj[i] = pop();
			push(Arrays.hashCode(obj));
		}
		@Override
		public String toString() {
			return String.format("ArrayHasher(%d)", n);
		}
	}

	public static final class ArrayListHasher extends Filter<Object, Integer> {
		private final int n;
		public ArrayListHasher(int n) {
			super(n, 1);
			this.n = n;
		}
		@Override
		public void work() {
			List<Object> list = new ArrayList<>(n);
			for (int i = 0; i < n; ++i)
				list.add(pop());
			push(list.hashCode());
		}
		@Override
		public String toString() {
			return String.format("ArrayListHasher(%d)", n);
		}
	}

	public static final class PeekingAdder extends Filter<Integer, Integer> {
		private final int n;
		public PeekingAdder(int n) {
			super(1, 1, n);
			this.n = n;
		}
		@Override
		public void work() {
			int sum = 0;
			for (int i = 0; i < n; ++i)
				sum += peek(i);
			push(sum);
			pop();
		}
		@Override
		public String toString() {
			return String.format("PeekingAdder(%d)", n);
		}
	}
}
