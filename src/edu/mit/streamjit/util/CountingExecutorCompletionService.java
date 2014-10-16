package edu.mit.streamjit.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An ExecutorCompletionService that keeps a count of tasks submitted but not
 * yet removed from the completion service.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/2/2013
 */
public class CountingExecutorCompletionService<V> extends ExecutorCompletionService<V> {
	private final AtomicInteger count = new AtomicInteger(0);
	public CountingExecutorCompletionService(Executor executor) {
		super(executor);
	}
	public CountingExecutorCompletionService(Executor executor, BlockingQueue<Future<V>> completionQueue) {
		super(executor, completionQueue);
	}

	@Override
	public Future<V> submit(Callable<V> task) {
		Future<V> f = super.submit(task);
		count.incrementAndGet();
		return f;
	}

	@Override
	public Future<V> submit(Runnable task, V result) {
		Future<V> f = super.submit(task, result);
		count.incrementAndGet();
		return f;
	}

	@Override
	public Future<V> take() throws InterruptedException {
		Future<V> f = super.take();
		count.decrementAndGet();
		return f;
	}

	@Override
	public Future<V> poll() {
		Future<V> f = super.poll();
		if (f != null)
			count.decrementAndGet();
		return f;
	}

	@Override
	public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
		Future<V> f = super.poll(timeout, unit);
		if (f != null)
			count.decrementAndGet();
		return f;
	}

	/**
	 * Returns the number of tasks submitted to this CompletionService that have
	 * not yet been removed.  Note that the return value is immediately stale,
	 * as other threads may have submitted or removed tasks concurrently.
	 * @return the number of pending tasks
	 */
	public int pendingTasks() {
		return count.get();
	}
}
