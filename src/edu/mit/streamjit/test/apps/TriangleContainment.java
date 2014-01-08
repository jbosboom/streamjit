package edu.mit.streamjit.test.apps;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmarker;
import java.awt.Polygon;
import java.util.Collections;
import java.util.Random;

/**
 * Parses string representations of triangles, then tests whether they contain
 * the origin.  Result is a sequence of booleans representing whether the
 * triangle contained the origin or not.  Loosely based on Project Euler problem
 * 102, "Triangle Containment".
 *
 * This file also contains an implementation using ForkJoinPool, for
 * comparison purposes.  It simply computes a count of origin-containing
 * triangles, rather than a list of booleans.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/7/2014
 */
public final class TriangleContainment {
	private static final int TRIANGLE_SIDES = 3;
	private TriangleContainment() {}

	private static final class Parser extends Filter<String, Integer> {
		private Parser() {
			super(1, TRIANGLE_SIDES*2);
		}
		@Override
		public void work() {
			for (String s : pop().split(","))
				push(Integer.valueOf(s));
		}
	}

	private static final class OriginTester extends Filter<Integer, Boolean> {
		private OriginTester() {
			super(TRIANGLE_SIDES*2, 1);
		}
		@Override
		public void work() {
			Polygon p = new Polygon();
			for (int i = 0; i < TRIANGLE_SIDES; ++i)
				p.addPoint(pop(), pop());
			push(p.contains(0, 0));
		}
	}

	private static final class TriangleContainmentBenchmark extends AbstractBenchmark {
		private TriangleContainmentBenchmark() {
			super(new Dataset("triangles", Input.fromIterable(generateInput())));
		}
		@Override
		@SuppressWarnings("unchecked")
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline(new Parser(), new OriginTester());
		}
	}

	private static final int NUM_TRIANGLES = 10000;
	private static final int REPETITIONS = 5000;
	private static Iterable<String> generateInput() {
		Random rng = new Random(0);
		ImmutableList.Builder<String> list = ImmutableList.builder();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < NUM_TRIANGLES; ++i) {
			sb.append(rng.nextInt(2001)-1000).append(",").append(rng.nextInt(2001)-1000);
			for (int j = 1; j < TRIANGLE_SIDES; ++j)
				sb.append(",").append(rng.nextInt(2001)-1000).append(",").append(rng.nextInt(2001)-1000);
			list.add(sb.toString());
			sb.delete(0, sb.length());
		}
		return Iterables.concat(Collections.nCopies(REPETITIONS, list.build()));
	}

	public static void main(String[] args) {
//		StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new Compiler2StreamCompiler().maxNumCores(4).multiplier(Short.MAX_VALUE);
		Benchmarker.runBenchmark(new TriangleContainmentBenchmark(), sc).get(0).print(System.out);
	}
}
