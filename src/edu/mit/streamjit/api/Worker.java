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
package edu.mit.streamjit.api;

import edu.mit.streamjit.impl.interp.Message;
import edu.mit.streamjit.impl.interp.Channel;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Worker is a StreamElement with a work function, rates, and the
 * ability to receive teleport messages via a Portal.
 * <p/>
 * Ideally, subclasses would provide their rates to a Worker
 * constructor. But that would require splitters and joiners to commit to their
 * rates before knowing how many outputs/inputs they have. If the user wants to
 * build a splitjoin with a variable number of elements, he or she would have to
 * figure out how many before constructing the splitjoin, rather than being able
 * to call Splitjoin.add() in a loop. Even if the user wants a fixed number of
 * elements in the splitjoin, he or she shouldn't have to repeat him or herself
 * by specifying a splitter size, joiner size and then adding that many elements
 * (classic StreamIt doesn't require this, for example). Thus, rates are instead
 * provided by abstract methods. Note that Filter, being
 * single-input-single-output, does manage its rates for you.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/19/2012
 */
public abstract class Worker<I, O> implements StreamElement<I, O> {
	public abstract void work();

	/**
	 * Returns a list of peek rates, such that the rate for the i-th channel is
	 * returned at index i.  Workers with only one input will return a singleton
	 * list.
	 * @return a list of peek rates
	 */
	public abstract List<Rate> getPeekRates();

	/**
	 * Returns a list of pop rates, such that the rate for the i-th channel is
	 * returned at index i.  Workers with only one input will return a singleton
	 * list.
	 * @return a list of pop rates
	 */
	public abstract List<Rate> getPopRates();

	/**
	 * Returns a list of push rates, such that the rate for the i-th channel is
	 * returned at index i. Workers with only one output will return a singleton
	 * list.
	 * @return a list of push rates
	 */
	public abstract List<Rate> getPushRates();

	private final List<Worker<?, ? extends I>> predecessors = new ArrayList<>(1);
	private final List<Worker<? super O, ?>> successors = new ArrayList<>(1);
	private final List<Channel<? extends I>> inputChannels = new ArrayList<>(1);
	private final List<Channel<? super O>> outputChannels = new ArrayList<>(1);
	private final List<Message> messages = new ArrayList<>();
	private long executions;
	/**
	 * An entirely arbitrary identifier, unique in a stream graph, to support
	 * maintaining worker identity in the distributed implementation.
	 */
	private int identifier = -1;

	void addPredecessor(Worker<?, ? extends I> predecessor, Channel<? extends I> channel) {
		if (predecessor == null)
			throw new NullPointerException();
		if (predecessor == this)
			throw new IllegalArgumentException();
		predecessors.add(predecessor);
		inputChannels.add(channel);
	}
	void addSuccessor(Worker<? super O, ?> successor, Channel<? super O> channel) {
		if (successor == null)
			throw new NullPointerException();
		if (successor == this)
			throw new IllegalArgumentException();
		successors.add(successor);
		outputChannels.add(channel);
	}

	List<Worker<?, ? extends I>> getPredecessors() {
		return predecessors;
	}

	List<Worker<? super O, ?>> getSuccessors() {
		return successors;
	}

	List<Channel<? extends I>> getInputChannels() {
		return inputChannels;
	}

	List<Channel<? super O>> getOutputChannels() {
		return outputChannels;
	}

	/**
	 * Called by interpreters to do work and other related tasks, such as
	 * processing messages.
	 */
	void doWork() {
		while (!messages.isEmpty() && messages.get(0).timeToReceive == executions+1) {
			Message m = messages.remove(0);
			try {
				m.method.invoke(this, m.args);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				throw new IllegalStreamGraphException("Bad stuff happened while processing message", ex);
			}
		}

		//TODO: implement prework here
		work();

		++executions;
	}

	/**
	 * Returns the number of completed executions of this worker.
	 * @return the number of completed executions of this worker.
	 */
	long getExecutions() {
		return executions;
	}

	void sendMessage(Message message) {
		if (message.timeToReceive <= executions)
			throw new AssertionError("Message delivery missed: "+executions+", "+message);
		//Insert in order sorted by time-to-receive.
		int insertionPoint = Collections.binarySearch(messages, message);
		if (insertionPoint < 0)
			insertionPoint = -(insertionPoint + 1);
		messages.add(insertionPoint, message);
	}

	int getIdentifier() {
		return identifier;
	}

	void setIdentifier(int identifier) {
		this.identifier = identifier;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "@" + (identifier != -1 ? identifier : hashCode());
	}

	//<editor-fold defaultstate="collapsed" desc="Friend pattern support (see impl.common.Workers)">
	private static class WorkersFriend extends edu.mit.streamjit.impl.common.Workers {
		@Override
		protected <I> void addPredecessor_impl(Worker<I, ?> worker, Worker<?, ? extends I> predecessor, Channel<? extends I> channel) {
			worker.addPredecessor(predecessor, channel);
		}
		@Override
		protected <O> void addSuccessor_impl(Worker<?, O> worker, Worker<? super O, ?> successor, Channel<? super O> channel) {
			worker.addSuccessor(successor, channel);
		}
		@Override
		protected <I> List<Worker<?, ? extends I>> getPredecessors_impl(Worker<I, ?> worker) {
			return worker.getPredecessors();
		}
		@Override
		protected <O> List<Worker<? super O, ?>> getSuccessors_impl(Worker<?, O> worker) {
			return worker.getSuccessors();
		}
		@Override
		protected <I> List<Channel<? extends I>> getInputChannels_impl(Worker<I, ?> worker) {
			return worker.getInputChannels();
		}
		@Override
		protected <O> List<Channel<? super O>> getOutputChannels_impl(Worker<?, O> worker) {
			return worker.getOutputChannels();
		}
		@Override
		protected long getExecutions_impl(Worker<?, ?> worker) {
			return worker.getExecutions();
		}
		@Override
		protected void doWork_impl(Worker<?, ?> worker) {
			worker.doWork();
		}
		@Override
		protected void sendMessage_impl(Worker<?, ?> worker, Message message) {
			worker.sendMessage(message);
		}
		@Override
		protected int getIdentifier_impl(Worker<?, ?> worker) {
			return worker.getIdentifier();
		}
		@Override
		protected void setIdentifier_impl(Worker<?, ?> worker, int identifier) {
			worker.setIdentifier(identifier);
		}
		private static void init() {
			edu.mit.streamjit.impl.common.Workers.setFriend(new WorkersFriend());
		}
	}
	static {
		WorkersFriend.init();
	}
	//</editor-fold>
}
