package edu.mit.streamjit.apps.bitonicsort;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

/**
 * Moved from StreamIt's asplos06 benchmark. Refer STREAMIT_HOME/apps/benchmarks/asplos06/bitonic-sort/streamit/BitonicSort2.str for
 * original implementations.
 * @author Sumanan sumanan@mit.edu
 * @since Mar 12, 2013 
 */

/**
 * BitonicSort.java - Batcher's bitonic sort network Implementation works only for power-of-2 sizes starting from 2.
 * 
 * Note: 1. Each input element is also referred to as a key in the comments in this file. 2. BitonicSort of N keys is done using logN
 * merge stages and each merge stage is made up of lopP steps (P goes like 2, 4, ... N for the logN merge stages)
 * 
 * See Knuth "The Art of Computer Programming" Section 5.3.4 - "Networks for Sorting" (particularly the diagram titled "A nonstandard
 * sorting network based on bitonic sorting" in the First Set of Exercises - Fig 56 in second edition) Here is an online reference:
 * http://www.iti.fh-flensburg.de/lang/algorithmen/sortieren/bitonic/bitonicen.htm
 */
public class BitonicSort {
	public static void main(String[] args) throws InterruptedException {

		/* Make sure N is a power_of_2 */
		int N = 8;

		BitonicSort2 kernel = new BitonicSort2();
		// StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new ConcurrentStreamCompiler(6);
		//StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<Integer, Integer> stream = sc.compile(kernel);
		Integer output;
		for (int i = N * N * N * N; i > 0;) {
			if (stream.offer(i)) {
				// System.out.println("Offer success " + i);
				i--;
			} else {
				Thread.sleep(10);
			}
			while ((output = stream.poll()) != null)
				System.out.println(output);
		}
	//	Thread.sleep(10000);
		stream.drain();
		while(!stream.isDrained())
			while ((output = stream.poll()) != null)
				System.out.println(output);
		
		while ((output = stream.poll()) != null)
			System.out.println(output);
	}

	/**
	 * Compares the two input keys and exchanges their order if they are not sorted.
	 * 
	 * sortdir determines if the sort is nondecreasing (UP) or nonincreasing (DOWN). 'true' indicates UP sort and 'false' indicates
	 * DOWN sort.
	 */
	private static class CompareExchange extends Filter<Integer, Integer> {
		private boolean sortdir;

		public CompareExchange(boolean sortdir) {
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
	 * Partition the input bitonic sequence of length L into two bitonic sequences of length L/2, with all numbers in the first
	 * sequence <= all numbers in the second sequence if sortdir is UP (similar case for DOWN sortdir)
	 * 
	 * Graphically, it is a bunch of CompareExchanges with same sortdir, clustered together in the sort network at a particular step
	 * (of some merge stage).
	 */
	private static class PartitionBitonicSequence extends Splitjoin<Integer, Integer> {
		/* Each CompareExchange examines keys that are L/2 elements apart */
		PartitionBitonicSequence(int L, boolean sortdir) {
			super(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>());
			for (int i = 0; i < (L / 2); i++) {
				add(new CompareExchange(sortdir));
			}
		}
	}

	/**
	 * One step of a particular merge stage (used by all merge stages except the last)
	 * 
	 * dircnt determines which step we are in the current merge stage (which in turn is determined by <L, numseqp>)
	 */
	private static class StepOfMerge extends Splitjoin<Integer, Integer> {
		int L;
		int numseqp;
		int dircnt;

		private boolean curdir;

		StepOfMerge(int L, int numseqp, int dircnt) {
			super(new RoundrobinSplitter<Integer>(L), new RoundrobinJoiner<Integer>(L));
			this.L = L;
			this.numseqp = numseqp;
			this.dircnt = dircnt;
			addFilters();
		}

		private void addFilters() {
			for (int j = 0; j < numseqp; j++) {
				/*
				 * finding out the curdir is a bit tricky - the direction depends only on the subsequence number during the FIRST step.
				 * So to determine the FIRST step subsequence to which this sequence belongs, divide this sequence's number j by dircnt
				 * (bcoz 'dircnt' tells how many subsequences of the current step make up one subsequence of the FIRST step). Then,
				 * test if that result is even or odd to determine if curdir is UP or DOWN respec.
				 */
				curdir = ((j / dircnt) % 2 == 0);
				/*
				 * The last step needs special care to avoid splitjoins with just one branch.
				 */
				if (L > 2)
					add(new PartitionBitonicSequence(this.L, this.curdir));
				else
					/*
					 * PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
					 */
					add(new CompareExchange(this.curdir));
			}
		}
	}

	/**
	 * One step of the last merge stage
	 * 
	 * Main difference form StepOfMerge is the direction of sort. It is always in the same direction - sortdir.
	 */
	private static class StepOfLastMerge extends Splitjoin<Integer, Integer> {

		int L;
		int numseqp;
		boolean sortdir;

		StepOfLastMerge(int L, int numseqp, boolean sortdir) {
			super(new RoundrobinSplitter<Integer>(L), new RoundrobinJoiner<Integer>(L));
			this.L = L;
			this.numseqp = numseqp;
			this.sortdir = sortdir;
			addFilters();
		}

		private void addFilters() {
			for (int j = 0; j < numseqp; j++) {
				/*
				 * The last step needs special care to avoid splitjoins with just one branch.
				 */
				if (L > 2)
					add(new PartitionBitonicSequence(L, sortdir));
				else
					/*
					 * PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
					 */
					add(new CompareExchange(sortdir));
			}
		}
	}

	/*
	 * Divide the input sequence of length N into subsequences of length P and sort each of them (either UP or DOWN depending on what
	 * subsequence number [0 to N/P-1] they get - All even subsequences are sorted UP and all odd subsequences are sorted DOWN) In
	 * short, a MergeStage is N/P Bitonic Sorters of order P each.
	 * 
	 * But, this MergeStage is implemented *iteratively* as logP STEPS.
	 */
	private static class MergeStage extends Pipeline<Integer, Integer> {
		int P;
		int N;
		int L, numseqp, dircnt;

		MergeStage(int P, int N) {
			this.P = P;
			this.N = N;
			addFilters();
		}

		private void addFilters() {
			/*
			 * for each of the lopP steps (except the last step) of this merge stage
			 */
			for (int i = 1; i < P; i = i * 2) {
				/*
				 * length of each sequence for the current step - goes like P,P/2,...,2
				 */
				L = P / i;
				/*
				 * numseqp is the number of PartitionBitonicSequence-rs in this step
				 */
				numseqp = (N / P) * i;
				dircnt = i;

				add(new StepOfMerge(L, numseqp, dircnt));
			}
		}

	}

	/**
	 * The LastMergeStage is basically one Bitonic Sorter of order N i.e., it takes the bitonic sequence produced by the previous merge
	 * stages and applies a bitonic merge on it to produce the final sorted sequence.
	 * 
	 * This is implemented iteratively as logN steps
	 */
	private static class LastMergeStage extends Pipeline<Integer, Integer> {
		int N;
		boolean sortdir;
		int L, numseqp;

		LastMergeStage(int N, boolean sortdir) {
			this.N = N;
			this.sortdir = sortdir;
			addFilter();

		}

		private void addFilter() {
			/*
			 * for each of the logN steps (except the last step) of this merge stage
			 */
			for (int i = 1; i < N; i = i * 2) {
				/*
				 * length of each sequence for the current step - goes like N,N/2,...,2
				 */
				L = N / i;
				/*
				 * numseqp is the number of PartitionBitonicSequence-rs in this step
				 */
				numseqp = i;

				add(new StepOfLastMerge(L, numseqp, sortdir));
			}
		}
	}

	/**
	 * The top-level kernel of bitonic-sort (iterative version) - It has logN merge stages and all merge stages except the last
	 * progressively builds a bitonic sequence out of the input sequence. The last merge stage acts on the resultant bitonic sequence
	 * to produce the final sorted sequence (sortdir determines if it is UP or DOWN).
	 */
	private static class BitonicSortKernel extends Pipeline<Integer, Integer> {
		int N;
		boolean sortdir;

		BitonicSortKernel(int N, boolean sortdir) {
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

	/**
	 * Creates N keys and sends it out
	 */
	private static class KeySource extends Filter<Void, Integer> {
		int[] A;
		int N;

		KeySource(int N) {
			super(0, N);
			this.N = N;
			A = new int[N];
			init();
		}

		private void init() {

			/*
			 * Initialize the input. In future, might want to read from file or generate a random permutation.
			 */
			for (int i = 0; i < N; i++)
				A[i] = (N - i);
		}

		@Override
		public void work() {
			for (int i = 0; i < N; i++)
				push(A[i]);

		}
	}

	/**
	 * Prints out the sorted keys and verifies if they are sorted.
	 */
	private static class KeyPrinter extends Filter<Integer, Void> {
		int N;

		KeyPrinter(int N) {
			super(N, 0);
			this.N = N;
		}

		public void work() {
			for (int i = 0; i < (N - 1); i++) {
				/* verifies if it is UP sorted */
				// ASSERT(peek(0) <= peek(1));
				/* verifies if it is DOWN sorted */
				// ASSERT(peek(0) >= peek(1));
				System.out.println(pop());
			}
			System.out.println(pop());
		}
	}

	/**
	 * The driver class FIXME: Original is "void->void pipeline BitonicSort2". As StreamJit currently doesn't support FileReader<?>,
	 * FileWriter<?> and void input to the source worker, implementation is bit changed here. But anyway these need to be fixed soon.
	 * Correct class definition should be "private static class BitonicSort2 extends Pipeline<Void, Void>"
	 */
	public static class BitonicSort2 extends Pipeline<Integer, Integer> {
		/* Make sure N is a power_of_2 */
		int N = 8;
		/* true for UP sort and false for DOWN sort */
		boolean sortdir = true;

		public BitonicSort2() {
			// add KeySource(N);
			// add FileReader<int>("../input/BitonicSort2.in");
			add(new BitonicSortKernel(N, sortdir));
			// add BitonicSortKernel(N, !sortdir);
			// add KeyPrinter(N);
			// add FileWriter<int>("BitonicSort2.out");
		}
	}
}
