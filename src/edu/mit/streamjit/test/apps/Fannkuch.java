package edu.mit.streamjit.test.apps;

import com.google.common.collect.Collections2;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * http://benchmarksgame.alioth.debian.org/u32/performance.php?test=fannkuchredux#about
 * for details.  It would be faster to use int[] rather than List<Integer> but
 * that would be less convenient.  (Maybe we could even unbox it, by noticing we
 * never mutate the list size.)
 *
 * Note that this stream doesn't produce an output, but instead accumulates a
 * result in the Max filter.  Also note that we don't want to fuse the filters
 * because we can data-parallelize the first one.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/15/2014
 */
public final class Fannkuch {
	private Fannkuch() {}

	public static final class FannkuchBenchmark extends AbstractBenchmark {
		private static final int LIST_SIZE = 11;
		public FannkuchBenchmark() {
			super("Fannkuch", Datasets.fromIterable("permutations(1.."+LIST_SIZE+")",
					Collections2.permutations(ContiguousSet.create(Range.closed(1, LIST_SIZE), DiscreteDomain.integers()))));
		}
		@Override
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline(new FannkuchFilter(), new Max());
		}
	}

	private static final class FannkuchFilter extends Filter<List<Integer>, Integer> {
		private FannkuchFilter() {
			super(1, 1);
		}
		@Override
		public void work() {
			//Guava returns immutable lists, so we must copy.
			List<Integer> list = new ArrayList<>(pop());
			int flips = 0;
			while (list.get(0) != 1) {
				Collections.reverse(list.subList(0, list.get(0)));
				++flips;
			}
			push(flips);
		}
	}

	private static final class Max extends StatefulFilter<Integer, Void> {
		private int max = Integer.MIN_VALUE;
		private Max() {
			super(1, 0);
		}
		@Override
		public void work() {
			max = Math.max(max, pop());
		}
		public int max() {
			return max;
		}
	}

	public static void main(String[] args) {
//		StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new Compiler2StreamCompiler().maxNumCores(4).multiplier(256);
		Benchmarker.runBenchmark(new FannkuchBenchmark(), sc).get(0).print(System.out);
	}
}
