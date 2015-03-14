/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.interp.Message;
import edu.mit.streamjit.util.TopologicalSort;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * This class provides static utility methods for Worker, including
 * access to package-private members.
 *
 * This class is not final, but should not be subclassed.  (Its only subclass
 * is to allow access to package-private members.)
 *
 * For details on the "friend"-like pattern used here, see Practical API Design:
 * Confessions of a Java Framework Architect by Jaroslav Tulach, page 76.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/14/2013
 */
public abstract class Workers {
	public static <I> void addPredecessor(Worker<I, ?> worker, Worker<?, ? extends I> predecessor, Channel<? extends I> channel) {
		FRIEND.addPredecessor_impl(worker, predecessor, channel);
	}
	public static <O> void addSuccessor(Worker<?, O> worker, Worker<? super O, ?> successor, Channel<? super O> channel) {
		FRIEND.addSuccessor_impl(worker, successor, channel);
	}
	public static <I> List<Worker<?, ? extends I>> getPredecessors(Worker<I, ?> worker) {
		return FRIEND.getPredecessors_impl(worker);
	}
	public static <O> List<Worker<? super O, ?>> getSuccessors(Worker<?, O> worker) {
		return FRIEND.getSuccessors_impl(worker);
	}
	public static <I> List<Channel<? extends I>> getInputChannels(Worker<I, ?> worker) {
		return FRIEND.getInputChannels_impl(worker);
	}
	public static <O> List<Channel<? super O>> getOutputChannels(Worker<?, O> worker) {
		return FRIEND.getOutputChannels_impl(worker);
	}
	public static long getExecutions(Worker<?, ?> worker) {
		return FRIEND.getExecutions_impl(worker);
	}
	public static void doWork(Worker<?, ?> worker) {
		FRIEND.doWork_impl(worker);
	}
	public static void sendMessage(Worker<?, ?> worker, Message message) {
		FRIEND.sendMessage_impl(worker, message);
	}
	public static int getIdentifier(Worker<?, ?> worker) {
		return FRIEND.getIdentifier_impl(worker);
	}
	public static void setIdentifier(Worker<?, ?> worker, int identifier) {
		FRIEND.setIdentifier_impl(worker, identifier);
	}

	/**
	 * Returns a set of all predecessors of this worker.
	 * @param worker a worker
	 * @return a set of all predecessors of this worker
	 */
	public static ImmutableSet<Worker<?, ?>> getAllPredecessors(Worker<?, ?> worker) {
		Queue<Worker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(Workers.getPredecessors(worker));
		Set<Worker<?, ?>> closed = new HashSet<>(frontier);
		while (!frontier.isEmpty()) {
			Worker<?, ?> cur = frontier.remove();
			for (Worker<?, ?> w : Workers.getPredecessors(cur))
				if (!closed.contains(w)) {
					frontier.add(w);
					closed.add(w);
				}
		}
		return ImmutableSet.copyOf(closed);
	}

	/**
	 * Returns a set of all successors of this worker.
	 * @param worker a worker
	 * @return a set of all successors of this worker
	 */
	public static ImmutableSet<Worker<?, ?>> getAllSuccessors(Worker<?, ?> worker) {
		Queue<Worker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(Workers.getSuccessors(worker));
		Set<Worker<?, ?>> closed = new HashSet<>(frontier);
		while (!frontier.isEmpty()) {
			Worker<?, ?> cur = frontier.remove();
			for (Worker<?, ?> w : Workers.getSuccessors(cur))
				if (!closed.contains(w)) {
					frontier.add(w);
					closed.add(w);
				}
		}
		return ImmutableSet.copyOf(closed);
	}

	/**
	 * Returns a set of all workers in the stream graph that this worker is
	 * part of, including the worker itself.
	 * @param worker a worker
	 * @return a set of all workers in the stream graph
	 */
	public static ImmutableSet<Worker<?, ?>> getAllWorkersInGraph(Worker<?, ?> worker) {
		return ImmutableSet.<Worker<?, ?>>builder()
				.add(worker)
				.addAll(getAllPredecessors(worker))
				.addAll(getAllSuccessors(worker))
				.build();
	}

	/**
	 * Topologically sort the given set of nodes, such that each node precedes
	 * all of its successors in the returned list.
	 * @param nodes the set of nodes to sort
	 * @return a topologically-ordered list of the given nodes
	 */
	public static <T extends Worker<?, ?>> ImmutableList<T> topologicalSort(Iterable<T> workers) {
		return TopologicalSort.sort(workers, (a, b) -> Workers.getAllSuccessors(a).contains(b));
	}

	/**
	 * Returns a set of the topmost workers in the given set (workers that are
	 * not the successor of any worker in the set). The returned set may contain
	 * any number of workers between 0 and the given set's size, but will
	 * contain 0 workers only when the given set is empty.
	 * @param workers a set of workers
	 * @return a set of the topmost workers
	 */
	public static ImmutableSet<Worker<?, ?>> getTopmostWorkers(Set<Worker<?, ?>> workers) {
		Set<Worker<?, ?>> topmost = new HashSet<>(workers);
		for (Worker<?, ?> w : workers)
			//We don't need to limit the removals to successors in the set.
			topmost.removeAll(Workers.getSuccessors(w));
		return ImmutableSet.copyOf(topmost);
	}

	/**
	 * Returns a set of the bottommost workers in the given set (workers that are
	 * not the predecessor of any worker in the set). The returned set may contain
	 * any number of workers between 0 and the given set's size, but will
	 * contain 0 workers only when the given set is empty.
	 * @param workers a set of workers
	 * @return a set of the bottommost workers
	 */
	public static ImmutableSet<Worker<?, ?>> getBottommostWorkers(Set<Worker<?, ?>> workers) {
		Set<Worker<?, ?>> bottommost = new HashSet<>(workers);
		for (Worker<?, ?> w : workers)
			//We don't need to limit the removals to successors in the set.
			bottommost.removeAll(Workers.getPredecessors(w));
		return ImmutableSet.copyOf(bottommost);
	}

	public enum StreamPosition {
		UPSTREAM, DOWNSTREAM, EQUAL, INCOMPARABLE;
		public StreamPosition opposite() {
			switch (this) {
				case UPSTREAM:
					return DOWNSTREAM;
				case DOWNSTREAM:
					return UPSTREAM;
				case EQUAL:
				case INCOMPARABLE:
					return this;
				default:
					throw new AssertionError();
			}
		}
	}

	/**
	 * Compare the position of this worker to the given other worker. Returns
	 * UPSTREAM if this worker is upstream of the other worker, DOWNSTREAM if
	 * this worker is downstream of the other worker, EQUAL if this worker is
	 * the other worker (reference equality), or INCOMPARABLE if this worker is
	 * neither upstream nor downstream of the other worker (e.g., this and other
	 * are in parallel branches of a splitjoin).
	 *
	 * Obviously, this method requires the workers in a stream graph be
	 * connected.  See ConnectPrimitiveWorkersVisitor.
	 * @param left the first worker
	 * @param right the second worker
	 * @return a StreamPosition
	 */
	public static StreamPosition compareStreamPosition(Worker<?, ?> left, Worker<?, ?> right) {
		if (left == null || right == null)
			throw new NullPointerException();
		if (left == right)
			return StreamPosition.EQUAL;

		Queue<Worker<?, ?>> frontier = new ArrayDeque<>();
		//BFS downstream.
		frontier.add(left);
		while (!frontier.isEmpty()) {
			Worker<?, ?> cur = frontier.remove();
			if (cur == right)
				return StreamPosition.UPSTREAM;
			frontier.addAll(Workers.getSuccessors(cur));
		}

		//BFS upstream.
		frontier.add(left);
		while (!frontier.isEmpty()) {
			Worker<?, ?> cur = frontier.remove();
			if (cur == right)
				return StreamPosition.DOWNSTREAM;
			frontier.addAll(Workers.getPredecessors(cur));
		}

		return StreamPosition.INCOMPARABLE;
	}

	/**
	 * Returns true iff the given worker is peeking (has max peek rate greater
	 * than max pop rate on at least one channel).
	 * @param worker the worker
	 * @return true iff this worker peeks
	 */
	public static boolean isPeeking(Worker<?, ?> worker) {
		for (int i = 0; i < worker.getPeekRates().size(); ++i)
			if (worker.getPeekRates().get(i).max() == Rate.DYNAMIC ||
					worker.getPeekRates().get(i).max() > worker.getPopRates().get(i).max())
				return true;
		return false;
	}

	//<editor-fold defaultstate="collapsed" desc="Friend pattern support">
	protected Workers() {}
	private static Workers FRIEND;
	protected static void setFriend(Workers workers) {
		if (FRIEND != null)
			throw new AssertionError("Can't happen: two friends?");
		FRIEND = workers;
	}
	static {
		try {
			//Ensure Worker is initialized.
			Class.forName(Worker.class.getName(), true, Worker.class.getClassLoader());
		} catch (ClassNotFoundException ex) {
			throw new AssertionError(ex);
		}
	}
	protected abstract <I> void addPredecessor_impl(Worker<I, ?> worker, Worker<?, ? extends I> predecessor, Channel<? extends I> channel);
	protected abstract <O> void addSuccessor_impl(Worker<?, O> worker, Worker<? super O, ?> successor, Channel<? super O> channel);
	protected abstract <I> List<Worker<?, ? extends I>> getPredecessors_impl(Worker<I, ?> worker);
	protected abstract <O> List<Worker<? super O, ?>> getSuccessors_impl(Worker<?, O> worker);
	protected abstract <I> List<Channel<? extends I>> getInputChannels_impl(Worker<I, ?> worker);
	protected abstract <O> List<Channel<? super O>> getOutputChannels_impl(Worker<?, O> worker);
	protected abstract long getExecutions_impl(Worker<?, ?> worker);
	protected abstract void doWork_impl(Worker<?, ?> worker);
	protected abstract void sendMessage_impl(Worker<?, ?> worker, Message message);
	protected abstract int getIdentifier_impl(Worker<?, ?> worker);
	protected abstract void setIdentifier_impl(Worker<?, ?> worker, int identifier);
	//</editor-fold>
}
