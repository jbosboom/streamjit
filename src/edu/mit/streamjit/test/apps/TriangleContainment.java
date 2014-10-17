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
package edu.mit.streamjit.test.apps;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parses string representations of triangles, then tests whether they contain
 * the origin.  Result is a sequence of booleans representing whether the
 * triangle contained the origin or not.  Loosely based on Project Euler problem
 * 102, "Triangle Containment".
 *
 * This file also contains an implementation using threads, for
 * comparison purposes.  It simply computes a count of origin-containing
 * triangles, rather than a list of booleans.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
				push(Integer.parseInt(s));
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

	private static final class ManuallyFused extends Filter<String, Boolean> {
		private ManuallyFused() {
			super(1, 1);
		}
		@Override
		public void work() {
			String[] data = pop().split(",");
			Polygon p = new Polygon();
			for (int i = 0; i < TRIANGLE_SIDES*2; i += 2)
				p.addPoint(Integer.parseInt(data[i]), Integer.parseInt(data[i+1]));
			push(p.contains(0, 0));
		}
	}

	@ServiceProvider(Benchmark.class)
	public static final class TriangleContainmentBenchmark extends AbstractBenchmark {
		public TriangleContainmentBenchmark() {
			super("TrangleContainment", new Dataset("triangles", Input.fromIterable(generateInput())));
		}
		@Override
		@SuppressWarnings("unchecked")
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline(new Parser(), new OriginTester());
		}
	}

	@ServiceProvider(Benchmark.class)
	public static final class ManuallyFusedTriangleContainmentBenchmark extends AbstractBenchmark {
		public ManuallyFusedTriangleContainmentBenchmark() {
			super("ManuallyFusedTrangleContainment", new Dataset("triangles", Input.fromIterable(generateInput())));
		}
		@Override
		@SuppressWarnings("unchecked")
		public OneToOneElement<Object, Object> instantiate() {
			return new Pipeline(new ManuallyFused());
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

	private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
	private static int runThreads() {
		Iterator<String> taskIterator = generateInput().iterator();
		AtomicInteger result = new AtomicInteger(0);
		List<Thread> threads = new ArrayList<>(NUM_THREADS);
		List<Semaphore> readSemaphores = new ArrayList<>(NUM_THREADS), writeSemaphores = new ArrayList<>(NUM_THREADS);
		for (int i = 0; i < NUM_THREADS; ++i) {
			readSemaphores.add(new Semaphore(i == 0 ? 1 : 0));
			writeSemaphores.add(new Semaphore(i == 0 ? 1 : 0));
		}
		for (int i = 0; i < NUM_THREADS; ++i)
			threads.add(new ComputeThread(taskIterator, result,
					readSemaphores.get(i), readSemaphores.get((i+1)%readSemaphores.size()),
					writeSemaphores.get(i), writeSemaphores.get((i+1)%writeSemaphores.size())));
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (Thread t : threads)
			t.start();
		for (Thread t : threads)
			Uninterruptibles.joinUninterruptibly(t);
		System.out.println("Thread impl ran in " + stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
		return result.get();
	}

	private static final class ComputeThread extends Thread {
		private static final int STRINGS_PER_TASK = 100000;
		private final Iterator<String> taskIterator;
		//A token-passing scheme to ensure tasks are issued and retired in order,
		//for fair comparison against StreamJIT.
		private final Semaphore readSemaphore, nextReadSemaphore, writeSemaphore, nextWriteSemaphore;
		private final AtomicInteger result;
		private ComputeThread(Iterator<String> taskIterator, AtomicInteger result, Semaphore readSemaphore, Semaphore nextReadSemaphore, Semaphore writeSemaphore, Semaphore nextWriteSemaphore) {
			this.taskIterator = taskIterator;
			this.result = result;
			this.readSemaphore = readSemaphore;
			this.nextReadSemaphore = nextReadSemaphore;
			this.writeSemaphore = writeSemaphore;
			this.nextWriteSemaphore = nextWriteSemaphore;
		}

		@Override
		public void run() {
			List<String> tasks = new ArrayList<>(STRINGS_PER_TASK);
			while (true) {
				tasks.clear();
				readSemaphore.acquireUninterruptibly();
				for (int i = 0; i < STRINGS_PER_TASK && taskIterator.hasNext(); ++i)
					tasks.add(taskIterator.next());
				nextReadSemaphore.release();
				if (tasks.isEmpty())
					return;

				int accum = 0;
				for (String task : tasks) {
					String[] data = task.split(",");
					Polygon p = new Polygon();
					for (int i = 0; i < TRIANGLE_SIDES*2; i += 2)
						p.addPoint(Integer.parseInt(data[i]), Integer.parseInt(data[i+1]));
					if (p.contains(0, 0))
						++accum;
				}

				writeSemaphore.acquireUninterruptibly();
				result.addAndGet(accum);
				nextWriteSemaphore.release();
			}
		}
	}

	public static void main(String[] args) {
//		StreamCompiler sc = new DebugStreamCompiler();
		StreamCompiler sc = new Compiler2StreamCompiler().maxNumCores(4).multiplier(Short.MAX_VALUE);
		Benchmarker.runBenchmark(new TriangleContainmentBenchmark(), sc).get(0).print(System.out);
		runThreads();
	}
}
