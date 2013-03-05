package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.PrimitiveWorker;
import edu.mit.streamjit.impl.interp.Message;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * This class provides static utility methods for PrimitiveWorker, including
 * access to package-private members.
 *
 * This class is not final, but should not be subclassed.  (Its only subclass
 * is to allow access to package-private members.)
 *
 * For details on the "friend"-like pattern used here, see Practical API Design:
 * Confessions of a Java Framework Architect by Jaroslav Tulach, page 76.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/14/2013
 */
public abstract class Workers {
	public static <I> void addPredecessor(PrimitiveWorker<I, ?> worker, PrimitiveWorker<?, ? extends I> predecessor, Channel<? extends I> channel) {
		FRIEND.addPredecessor_impl(worker, predecessor, channel);
	}
	public static <O> void addSuccessor(PrimitiveWorker<?, O> worker, PrimitiveWorker<? super O, ?> successor, Channel<? super O> channel) {
		FRIEND.addSuccessor_impl(worker, successor, channel);
	}
	public static <I> List<PrimitiveWorker<?, ? extends I>> getPredecessors(PrimitiveWorker<I, ?> worker) {
		return FRIEND.getPredecessors_impl(worker);
	}
	public static <O> List<PrimitiveWorker<? super O, ?>> getSuccessors(PrimitiveWorker<?, O> worker) {
		return FRIEND.getSuccessors_impl(worker);
	}
	public static <I> List<Channel<? extends I>> getInputChannels(PrimitiveWorker<I, ?> worker) {
		return FRIEND.getInputChannels_impl(worker);
	}
	public static <O> List<Channel<? super O>> getOutputChannels(PrimitiveWorker<?, O> worker) {
		return FRIEND.getOutputChannels_impl(worker);
	}
	public static long getExecutions(PrimitiveWorker<?, ?> worker) {
		return FRIEND.getExecutions_impl(worker);
	}
	public static void doWork(PrimitiveWorker<?, ?> worker) {
		FRIEND.doWork_impl(worker);
	}
	public static void sendMessage(PrimitiveWorker<?, ?> worker, Message message) {
		FRIEND.sendMessage_impl(worker, message);
	}

	/**
	 * Returns a set of all predecessors of this worker.
	 * @param worker a worker
	 * @return a set of all predecessors of this worker
	 */
	public static Set<PrimitiveWorker<?, ?>> getAllPredecessors(PrimitiveWorker<?, ?> worker) {
		Set<PrimitiveWorker<?, ?>> closed = new HashSet<>();
		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(Workers.getPredecessors(worker));
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			closed.add(cur);
			frontier.addAll(Workers.getPredecessors(cur));
		}
		return Collections.unmodifiableSet(closed);
	}

	/**
	 * Returns a set of all successors of this worker.
	 * @param worker a worker
	 * @return a set of all successors of this worker
	 */
	public static Set<PrimitiveWorker<?, ?>> getAllSuccessors(PrimitiveWorker<?, ?> worker) {
		Set<PrimitiveWorker<?, ?>> closed = new HashSet<>();
		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(Workers.getSuccessors(worker));
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			closed.add(cur);
			frontier.addAll(Workers.getSuccessors(cur));
		}
		return Collections.unmodifiableSet(closed);
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
	public static StreamPosition compareStreamPosition(PrimitiveWorker<?, ?> left, PrimitiveWorker<?, ?> right) {
		if (left == null || right == null)
			throw new NullPointerException();
		if (left == right)
			return StreamPosition.EQUAL;

		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		//BFS downstream.
		frontier.add(left);
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			if (cur == right)
				return StreamPosition.UPSTREAM;
			frontier.addAll(Workers.getSuccessors(cur));
		}

		//BFS upstream.
		frontier.add(left);
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			if (cur == right)
				return StreamPosition.DOWNSTREAM;
			frontier.addAll(Workers.getPredecessors(cur));
		}

		return StreamPosition.INCOMPARABLE;
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
			//Ensure PrimitiveWorker is initialized.
			Class.forName(PrimitiveWorker.class.getName(), true, PrimitiveWorker.class.getClassLoader());
		} catch (ClassNotFoundException ex) {
			throw new AssertionError(ex);
		}
	}
	protected abstract <I> void addPredecessor_impl(PrimitiveWorker<I, ?> worker, PrimitiveWorker<?, ? extends I> predecessor, Channel<? extends I> channel);
	protected abstract <O> void addSuccessor_impl(PrimitiveWorker<?, O> worker, PrimitiveWorker<? super O, ?> successor, Channel<? super O> channel);
	protected abstract <I> List<PrimitiveWorker<?, ? extends I>> getPredecessors_impl(PrimitiveWorker<I, ?> worker);
	protected abstract <O> List<PrimitiveWorker<? super O, ?>> getSuccessors_impl(PrimitiveWorker<?, O> worker);
	protected abstract <I> List<Channel<? extends I>> getInputChannels_impl(PrimitiveWorker<I, ?> worker);
	protected abstract <O> List<Channel<? super O>> getOutputChannels_impl(PrimitiveWorker<?, O> worker);
	protected abstract long getExecutions_impl(PrimitiveWorker<?, ?> worker);
	protected abstract void doWork_impl(PrimitiveWorker<?, ?> worker);
	protected abstract void sendMessage_impl(PrimitiveWorker<?, ?> worker, Message message);
	//</editor-fold>
}
