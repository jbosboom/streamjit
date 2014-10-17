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
package edu.mit.streamjit.test.apps.bitonicsort;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Moved from StreamIt's asplos06 benchmark. Refer STREAMIT_HOME/apps/benchmarks/asplos06/bitonic-sort/streamit/BitonicSort2.str for
 * original implementations.
 * @author Sumanan sumanan@mit.edu
 * @since Mar 12, 2013
 */

/**
 * BitonicSort.java - Batcher's bitonic sort network Implementation works only
 * for power-of-2 sizes starting from 2.
 *
 * <b>Note:</b>
 * <ol>
 * <li>Each input element is also referred to as a key in the comments in this
 * file.
 * <li>BitonicSort of N keys is done using logN merge stages and each merge
 * stage is made up of lopP steps (P goes like 2, 4, ... N for the logN merge
 * stages)
 * </ol>
 *
 * See Knuth "The Art of Computer Programming" Section 5.3.4 -
 * "Networks for Sorting" (particularly the diagram titled "A nonstandard
 * sorting network based on bitonic sorting" in the First Set of Exercises - Fig
 * 56 in second edition) Here is an online reference:
 * http://www.iti.fh-flensburg
 * .de/lang/algorithmen/sortieren/bitonic/bitonicen.htm
 */
@ServiceProvider(BenchmarkProvider.class)
public final class BitonicSort implements BenchmarkProvider {
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler().maxNumCores(8).multiplier(64);
		Benchmarker.runBenchmark(Iterables.getLast(new BitonicSort()), sc).get(0).print(System.out);
	}

	private static final int COPIES = 300;
	@Override
	public Iterator<Benchmark> iterator() {
		List<Benchmark> benchmarks = new ArrayList<>();
		for (int n : new int[]{4, 8, 16, 32})
			for (boolean ascending : new boolean[]{true, false}) {
				String filename = "BitonicSort"+n+".in";
				Input<Integer> input = Input.fromBinaryFile(Paths.get("data/"+filename), Integer.class, ByteOrder.LITTLE_ENDIAN);
				input = Datasets.nCopies(COPIES, input);
//				Input<Integer> output = Datasets.transformAll(new GroupSorter(n, ascending), input);
				Dataset data = new Dataset(filename, input);//.withOutput(output);
				String benchmarkName = String.format("BitonicSort (N = %d, %s)", n, ascending ? "asc" : "desc");
				benchmarks.add(new SuppliedBenchmark(benchmarkName, BitonicSortKernel.class, ImmutableList.of(n, ascending), data));
			}
		return benchmarks.iterator();
	}

	private static final class GroupSorter implements Function<Input<? super Integer>, Input<Integer>> {
		private final int n;
		private final boolean ascending;
		private GroupSorter(int n, boolean ascending) {
			this.n = n;
			this.ascending = ascending;
		}
		@Override
		public Input<Integer> apply(Input<? super Integer> input) {
			Buffer buffer = InputBufferFactory.unwrap(input).createReadableBuffer(n);
			Object[] items = new Object[n];
			List<Integer> retval = new ArrayList<>();
			while (buffer.readAll(items)) {
				if (ascending)
					Arrays.sort(items);
				else
					Arrays.sort(items, Collections.reverseOrder());
				for (Object o : items)
					retval.add((Integer)o);
			}
			return Input.fromIterable(retval);
		}
	}

	/**
	 * Compares the two input keys and exchanges their order if they are not
	 * sorted.
	 *
	 * sortdir determines if the sort is nondecreasing (UP) or nonincreasing
	 * (DOWN). 'true' indicates UP sort and 'false' indicates DOWN sort.
	 */
	private static final class CompareExchange extends Filter<Integer, Integer> {
		private final boolean sortdir;

		private CompareExchange(boolean sortdir) {
			super(2, 2);
			this.sortdir = sortdir;
		}

		@Override
		public void work() {
			/* the input keys and min,max keys */
			int k1, k2, mink, maxk;

			k1 = pop();
			k2 = pop();
			if (k1 <= k2) {
				mink = k1;
				maxk = k2;
			} else /* k1 > k2 */
			{
				mink = k2;
				maxk = k1;
			}

			if (sortdir == true) {
				/* UP sort */
				push(mink);
				push(maxk);
			} else /* sortdir == false */
			{
				/* DOWN sort */
				push(maxk);
				push(mink);
			}
		}

	}

	/**
	 * Partition the input bitonic sequence of length L into two bitonic
	 * sequences of length L/2, with all numbers in the first sequence <= all
	 * numbers in the second sequence if sortdir is UP (similar case for DOWN
	 * sortdir)
	 *
	 * Graphically, it is a bunch of CompareExchanges with same sortdir,
	 * clustered together in the sort network at a particular step (of some
	 * merge stage).
	 */
	private static final class PartitionBitonicSequence extends
			Splitjoin<Integer, Integer> {
		/* Each CompareExchange examines keys that are L/2 elements apart */
		private PartitionBitonicSequence(int L, boolean sortdir) {
			super(new RoundrobinSplitter<Integer>(),
					new RoundrobinJoiner<Integer>());
			for (int i = 0; i < (L / 2); i++) {
				add(new CompareExchange(sortdir));
			}
		}
	}

	/**
	 * One step of a particular merge stage (used by all merge stages except the
	 * last)
	 *
	 * dircnt determines which step we are in the current merge stage (which in
	 * turn is determined by <L, numseqp>)
	 */
	private static final class StepOfMerge extends Splitjoin<Integer, Integer> {
		private final int L;
		private final int numseqp;
		private final int dircnt;

		private StepOfMerge(int L, int numseqp, int dircnt) {
			super(new RoundrobinSplitter<Integer>(L),
					new RoundrobinJoiner<Integer>(L));
			this.L = L;
			this.numseqp = numseqp;
			this.dircnt = dircnt;
			addFilters();
		}

		private void addFilters() {
			boolean curdir;
			for (int j = 0; j < numseqp; j++) {
				/*
				 * finding out the curdir is a bit tricky - the direction
				 * depends only on the subsequence number during the FIRST step.
				 * So to determine the FIRST step subsequence to which this
				 * sequence belongs, divide this sequence's number j by dircnt
				 * (bcoz 'dircnt' tells how many subsequences of the current
				 * step make up one subsequence of the FIRST step). Then, test
				 * if that result is even or odd to determine if curdir is UP or
				 * DOWN respec.
				 */
				curdir = ((j / dircnt) % 2 == 0);
				/*
				 * The last step needs special care to avoid splitjoins with
				 * just one branch.
				 */
				if (L > 2)
					add(new PartitionBitonicSequence(this.L, curdir));
				else
					/*
					 * PartitionBitonicSequence of the last step (L=2) is simply
					 * a CompareExchange
					 */
					add(new CompareExchange(curdir));
			}
		}
	}

	/**
	 * One step of the last merge stage
	 *
	 * Main difference form StepOfMerge is the direction of sort. It is always
	 * in the same direction - sortdir.
	 */
	private static final class StepOfLastMerge extends Splitjoin<Integer, Integer> {
		private final int L;
		private final int numseqp;
		private final boolean sortdir;

		private StepOfLastMerge(int L, int numseqp, boolean sortdir) {
			super(new RoundrobinSplitter<Integer>(L),
					new RoundrobinJoiner<Integer>(L));
			this.L = L;
			this.numseqp = numseqp;
			this.sortdir = sortdir;
			addFilters();
		}

		private void addFilters() {
			for (int j = 0; j < numseqp; j++) {
				/*
				 * The last step needs special care to avoid splitjoins with
				 * just one branch.
				 */
				if (L > 2)
					add(new PartitionBitonicSequence(L, sortdir));
				else
					/*
					 * PartitionBitonicSequence of the last step (L=2) is simply
					 * a CompareExchange
					 */
					add(new CompareExchange(sortdir));
			}
		}
	}

	/*
	 * Divide the input sequence of length N into subsequences of length P and
	 * sort each of them (either UP or DOWN depending on what subsequence number
	 * [0 to N/P-1] they get - All even subsequences are sorted UP and all odd
	 * subsequences are sorted DOWN) In short, a MergeStage is N/P Bitonic
	 * Sorters of order P each.
	 *
	 * But, this MergeStage is implemented *iteratively* as logP STEPS.
	 */
	private static final class MergeStage extends Pipeline<Integer, Integer> {
		private final int P;
		private final int N;
		private MergeStage(int P, int N) {
			this.P = P;
			this.N = N;
			addFilters();
		}
		private void addFilters() {
			int L, numseqp, dircnt;
			/*
			 * for each of the lopP steps (except the last step) of this merge
			 * stage
			 */
			for (int i = 1; i < P; i = i * 2) {
				/*
				 * length of each sequence for the current step - goes like
				 * P,P/2,...,2
				 */
				L = P / i;
				/*
				 * numseqp is the number of PartitionBitonicSequence-rs in this
				 * step
				 */
				numseqp = (N / P) * i;
				dircnt = i;

				add(new StepOfMerge(L, numseqp, dircnt));
			}
		}

	}

	/**
	 * The LastMergeStage is basically one Bitonic Sorter of order N i.e., it
	 * takes the bitonic sequence produced by the previous merge stages and
	 * applies a bitonic merge on it to produce the final sorted sequence.
	 *
	 * This is implemented iteratively as logN steps
	 */
	private static final class LastMergeStage extends Pipeline<Integer, Integer> {
		private final int N;
		private final boolean sortdir;
		private LastMergeStage(int N, boolean sortdir) {
			this.N = N;
			this.sortdir = sortdir;
			addFilter();
		}
		private void addFilter() {
			int L, numseqp;
			/*
			 * for each of the logN steps (except the last step) of this merge
			 * stage
			 */
			for (int i = 1; i < N; i = i * 2) {
				/*
				 * length of each sequence for the current step - goes like
				 * N,N/2,...,2
				 */
				L = N / i;
				/*
				 * numseqp is the number of PartitionBitonicSequence-rs in this
				 * step
				 */
				numseqp = i;
				add(new StepOfLastMerge(L, numseqp, sortdir));
			}
		}
	}

	/**
	 * The top-level kernel of bitonic-sort (iterative version) - It has logN
	 * merge stages and all merge stages except the last progressively builds a
	 * bitonic sequence out of the input sequence. The last merge stage acts on
	 * the resultant bitonic sequence to produce the final sorted sequence
	 * (sortdir determines if it is UP or DOWN).
	 */
	public static final class BitonicSortKernel extends Pipeline<Integer, Integer> {
		private final int N;
		private final boolean sortdir;
		public BitonicSortKernel(int N, boolean sortdir) {
			this.N = N;
			this.sortdir = sortdir;
			addFilter();
		}
		private void addFilter() {
			for (int i = 2; i <= (N / 2); i = 2 * i) {
				add(new MergeStage(i, N));
			}
			add(new LastMergeStage(N, sortdir));
		}
	}
}
