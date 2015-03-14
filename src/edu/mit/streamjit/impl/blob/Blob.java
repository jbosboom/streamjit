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
package edu.mit.streamjit.impl.blob;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ComparisonChain;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Workers;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * A Blob represents a runnable part of a stream graph. Blobs are responsible
 * for some workers from the stream graph, have some inputs and outputs
 * identified by Tokens, and provide some number of Runnables that can be
 * executed on cores. Blob is confined into a single machine boundary (i.e, A
 * single blob never executed across multiple machines)
 * 
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/22/2013
 */
public interface Blob {
	/**
	 * Returns an immutable Set of the workers from the original stream graph
	 * this Blob contains (is responsible for).
	 * @return an immutable set of workers from the original stream graph this
	 * Blob contains
	 */
	public Set<Worker<?, ?>> getWorkers();

	/**
	 * Returns an immutable set of Tokens representing the input edges to this
	 * Blob.
	 * @return an immutable set of Tokens representing the input edges to this
	 * Blob.
	 */
	public Set<Token> getInputs();

	/**
	 * Returns an immutable set of Tokens representing the output edges to this
	 * Blob.
	 * @return an immutable set of Tokens representing the output edges to this
	 * Blob.
	 */
	public Set<Token> getOutputs();

	/**
	 * Returns this Blob's requested minimum capacity for the buffer on the
	 * edge represented by the given token.
	 * @param token the edge to get minimum capacity for
	 * @return the minimum capacity for the given edge
	 * @throws IllegalArgumentException if the given token is not an input or
	 * output edge of this Blob
	 */
	public int getMinimumBufferCapacity(Token token);

	/**
	 * Installs buffers for this Blob's input and output edges.
	 * @param buffers an immutable map of tokens to the buffer on the corresponding
	 * edge
	 * @throws IllegalArgumentException if the given map doesn't contain a mapping
	 * for one of this Blob's input or output edges
	 * @throws IllegalStateException if installBuffers is called more than once
	 */
	public void installBuffers(Map<Token, Buffer> buffers);

	/**
	 * Gets the number of cores that can run parts of this blob.
	 * @return the number of cores that can run parts of this blob
	 */
	public int getCoreCount();

	/**
	 * Gets a Runnable that the given core can run to run this blob. Cores index starts from 0.
	 * @param core the core to get a Runnable for
	 * @return a Runnable for part of this blob
	 */
	public Runnable getCoreCode(int core);

	/**
	 * Signals this Blob that its inputs have finished producing data and to
	 * drain its portion of the stream graph as fully as possible (produce as
	 * much output as possible), then call the given callback. As soon as this
	 * method is called, this Blob may assume no input will be added to its
	 * input channels, so once they are as empty as possible, the Blob may stop
	 * executing.
	 * <p/>
	 * This method may or may not block, at the discretion of the blob
	 * implementation. If it does block, it does so uninterruptibly.
	 * <p/>
	 * This method will cause the Runnables returned by getCoreCode() to stop
	 * doing useful work at some point in the future, but does not stop their
	 * threads.  The caller of drain() is responsible for stopping those threads
	 * after the callback is called.
	 * <p/>
	 * The callback may be called from any thread, including but not limited to
	 * the threads executing the Runnables returned by getCoreCode() and the
	 * thread calling drain(), so the callback should not assume anything about
	 * the thread it is executing on. In practice, this means the callback
	 * should signal another known thread to perform whatever work is required.
	 * <p/>
	 * Note that the callback is called when all output has been placed in the
	 * output channels, but if the next blob is on another machine, the data has
	 * not yet reached that blob's input channels yet (since it has to be sent
	 * over the network). The runtime system must take care not to call the next
	 * blob's drain() method until the data reaches that blob's input channels
	 * (i.e., the callback should not immediately call drain() without
	 * additional synchronization).
	 * <p/>
	 * The caller should interrupt the threads running the Blob's Runnables
	 * (from getCoreCode()) so they can observe the draining in case they are
	 * blocked.
	 * @param callback the callback to call after draining is finished
	 */
	public void drain(Runnable callback);

	/**
	 * Gets a DrainData representing the state of this Blob after draining. This
	 * method may only be called after the callback passed to drain() has been
	 * called.
	 *
	 * DrainData does not include data left in inter-Blob Buffers, which the
	 * runtime system already knows about and must track separately.
	 * @return DrainData for this blob
	 */
	public DrainData getDrainData();
	//TODO: getConfig()

	/**
	 * A Token represents an edge between two workers, suitable for identifying
	 * inputs and outputs of Blobs.  Tokens are serializable, but their meaning
	 * depends on the identifiers in Worker objects, so they're only useful when
	 * interpreted by Blobs involving the same canonical stream graph.
	 */
	public static class Token implements Serializable, Comparable<Token> {
		private static final long serialVersionUID = 1L;
		private final int upstreamIdentifier, downstreamIdentifier;
		private Token(int upstreamIdentifier, int downstreamIdentifier) {
			this.upstreamIdentifier = upstreamIdentifier;
			this.downstreamIdentifier = downstreamIdentifier;
		}
		/**
		 * Creates a Token representing the edge between the given two workers.
		 * @param upstream the upstream worker
		 * @param downstream the downstream worker
		 */
		public Token(Worker<?, ?> upstream, Worker<?, ?> downstream) {
			checkArgument(Workers.getSuccessors(upstream).contains(downstream));
			this.upstreamIdentifier = Workers.getIdentifier(upstream);
			this.downstreamIdentifier = Workers.getIdentifier(downstream);
			if (this.upstreamIdentifier == -1 || this.downstreamIdentifier == -1)
				throw new IllegalArgumentException("Workers must have identifiers before tokens can be created");
		}

		/**
		 * Creates a Token representing the overall input to the stream graph;
		 * that is, the edge leading to the first worker.
		 * @param firstWorker the first worker in the stream graph
		 * @return a Token representing the overall input to the stream graph
		 */
		public static Token createOverallInputToken(Worker<?, ?> firstWorker) {
			checkArgument(Workers.getIdentifier(firstWorker) != -1);
			checkArgument(Workers.getPredecessors(firstWorker).isEmpty(), "not actually first worker");
			return new Token(-1, Workers.getIdentifier(firstWorker));
		}

		/**
		 * Creates a Token representing the overall output of the stream graph;
		 * that is, the edge leading from the last worker.
		 * @param lastWorker the last worker in the stream graph
		 * @return a Token representing the overall output of the stream graph
		 */
		public static Token createOverallOutputToken(Worker<?, ?> lastWorker) {
			checkArgument(Workers.getIdentifier(lastWorker) != -1);
			checkArgument(Workers.getSuccessors(lastWorker).isEmpty(), "not actually last worker");
			return new Token(Workers.getIdentifier(lastWorker), -1);
		}

		/**
		 * Returns the identifier of the upstream worker, or -1 for the overall
		 * input of the stream graph.
		 * @return the identifier of the upstream worker, or -1
		 */
		public int getUpstreamIdentifier() {
			return upstreamIdentifier;
		}

		/**
		 * Returns the identifier of the downstream worker, or -1 for the
		 * overall output of the stream graph.
		 * @return the identifier of the downstream worker, or -1
		 */
		public int getDownstreamIdentifier() {
			return downstreamIdentifier;
		}

		public boolean isOverallInput() {
			return upstreamIdentifier == -1;
		}

		public boolean isOverallOutput() {
			return downstreamIdentifier == -1;
		}

		@Override
		public int compareTo(Token o) {
			return ComparisonChain.start()
					.compare(upstreamIdentifier, o.upstreamIdentifier)
					.compare(downstreamIdentifier, o.downstreamIdentifier)
					.result();
		}

		@Override
		public int hashCode() {
			int hash = 3;
			hash = 59 * hash + this.upstreamIdentifier;
			hash = 59 * hash + this.downstreamIdentifier;
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Token other = (Token)obj;
			if (this.upstreamIdentifier != other.upstreamIdentifier)
				return false;
			if (this.downstreamIdentifier != other.downstreamIdentifier)
				return false;
			return true;
		}

		@Override
		public String toString() {
			String up = upstreamIdentifier == -1 ? "input" : Integer.toString(upstreamIdentifier);
			String down = downstreamIdentifier == -1 ? "output" : Integer.toString(downstreamIdentifier);
			return String.format("Token(%s, %s)", up, down);
		}
	}
}
