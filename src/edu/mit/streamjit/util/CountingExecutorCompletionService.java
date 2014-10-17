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
