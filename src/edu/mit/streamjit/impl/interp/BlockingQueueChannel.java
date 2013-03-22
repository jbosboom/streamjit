package edu.mit.streamjit.impl.interp;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * A thread-safe Channel implementation based on a BlockingQueue.  Calls to push
 * and pop block until data is available.  InterruptedExceptions are thrown
 * despite Channel's methods not allowing checked exceptions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/22/2013
 */
public class BlockingQueueChannel<E> implements Channel<E> {
	private final BlockingQueue<E> queue;
	public BlockingQueueChannel(BlockingQueue<E> queue) {
		this.queue = checkNotNull(queue);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void push(E element) {
		try {
			queue.put(element);
		} catch (InterruptedException ex) {
			Thread.currentThread().stop(ex);
		}
	}

	@Override
	public E peek(int index) {
		return Iterables.get(queue, index);
	}

	@Override
	@SuppressWarnings("deprecation")
	public E pop() {
		try {
			return queue.take();
		} catch (InterruptedException ex) {
			Thread.currentThread().stop(ex);
			throw new AssertionError("Can't happen: unreachable statement");
		}
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return queue.iterator();
	}

	@Override
	public String toString() {
		return queue.toString();
	}
}
