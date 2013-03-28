package edu.mit.streamjit.impl.blob;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ComparisonChain;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.interp.Channel;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * A Blob represents a runnable part of a stream graph.  Blobs are responsible
 * for some workers from the stream graph, have some inputs and outputs
 * identified by Tokens, and provide some number of Runnables that can be
 * executed on cores.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
	 * Returns an immutable Map from tokens to the input channels of this blob.
	 * @return an immutable Map from tokens to the input channels of this blob
	 */
	public Map<Token, Channel<?>> getInputChannels();

	/**
	 * Returns an immutable Map from tokens to the output channels of this blob.
	 * @return an immutable Map from tokens to the output channels of this blob
	 */
	public Map<Token, Channel<?>> getOutputChannels();

	/**
	 * Gets the number of cores that can run parts of this blob.
	 * @return the number of cores that can run parts of this blob
	 */
	public int getCoreCount();

	/**
	 * Gets a Runnable that the given core can run to run this blob.
	 * @param core the core to get a Runnable for
	 * @return a Runnable for part of this blob
	 */
	public Runnable getCoreCode(int core);
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