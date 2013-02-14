package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.Channel;
import edu.mit.streamjit.PrimitiveWorker;
import java.util.List;

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
	//</editor-fold>
}
