/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.StatefulFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Some filters for tests.  Some of these may graduate to the API package in a
 * more general form (e.g., the math filters need to generalize to other types).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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

	public static final class StatefulAdder extends StatefulFilter<Integer, Integer> {
		private final int initialAddend;
		private int addend;
		public StatefulAdder(int initialAddend) {
			super(1, 1);
			this.addend = this.initialAddend = initialAddend;
		}
		@Override
		public void work() {
			push(pop() + (addend++));
		}
		@Override
		public String toString() {
			return String.format("StatefulAdder(%d)", initialAddend);
		}
	}

	public static final class StatefulMultiplier extends StatefulFilter<Integer, Integer> {
		private final int initialMultiplier;
		private int multiplier;
		public StatefulMultiplier(int multiplier) {
			super(1, 1);
			this.multiplier = this.initialMultiplier = multiplier;
		}
		@Override
		public void work() {
			push(pop() * (multiplier++));
		}
		@Override
		public String toString() {
			return String.format("StatefulMultiplier(%d)", initialMultiplier);
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

	public static final class ListGatherer<T> extends Filter<T, List<T>> {
		private final int n;
		public ListGatherer(int n) {
			super(n, 1);
			this.n = n;
		}
		@Override
		public void work() {
			List<T> list = new ArrayList<>(n);
			for (int i = 0; i < n; ++i)
				list.add(pop());
			push(list);
		}
		@Override
		public String toString() {
			return String.format("ListGatherer(%d)", n);
		}
	}

	public static final class ExtractMax<T extends Comparable<T>> extends Filter<Collection<T>, T> {
		public ExtractMax() {
			super(1, 1);
		}
		@Override
		public void work() {
			push(Collections.max(pop()));
		}
	}
}
