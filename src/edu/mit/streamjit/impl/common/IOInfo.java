package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.interp.Channel;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public class IOInfo {
	/**
	 * The kind of edge this IOInfo represents, in relation to the set of
	 * workers it was created from.
	 */
	private static enum ConnectionKind {
		/**
		 * An edge from a worker not in the set to a worker in the set.
		 */
		INPUT,
		/**
		 * An edge from a worker in the set to a worker not in the set.
		 */
		OUTPUT,
		/**
		 * An edge between two workers in the set.
		 */
		INTERNAL};
	private final Worker<?, ?> upstream;
	private final Worker<?, ?> downstream;
	private final Channel<?> channel;
	private final Blob.Token token;
	private final ConnectionKind connectionKind;

	private IOInfo(Worker<?, ?> upstream, Worker<?, ?> downstream, Channel<?> channel, Blob.Token token, ConnectionKind kind) {
		checkArgument(upstream != null || downstream != null);
		switch (kind) {
			case INPUT:
				checkArgument(downstream != null);
				break;
			case OUTPUT:
				checkArgument(upstream != null);
				break;
			case INTERNAL:
				checkArgument(upstream != null && downstream != null);
		}
		if (upstream != null && downstream != null)
			checkArgument(Workers.getSuccessors(upstream).contains(downstream));
		this.upstream = upstream;
		this.downstream = downstream;
		this.channel = channel;
		this.token = checkNotNull(token);
		this.connectionKind = kind;
	}

	/**
	 * Creates IOInfo objects for all edges of the given set of workers.  The workers
	 * must have their predecessor/successor relationships initialized, but they
	 * need not be connected with channels.  The given set need not be
	 * connected (in the reachability sense).
	 * @param workers a set of workers
	 * @return a set of IOInfo objects for all edges of the given set
	 */
	public static ImmutableSet<IOInfo> allEdges(Set<Worker<?, ?>> workers) {
		ImmutableSet.Builder<IOInfo> retval = ImmutableSet.builder();
		boolean overallInput = false;
		boolean overallOutput = false;
		for (Worker<?, ?> w : workers) {
			List<Worker<?, ?>> preds = (List<Worker<?, ?>>)Workers.getPredecessors(w);
			List<Channel<?>> ichans = (List<Channel<?>>)Workers.getInputChannels(w);
			checkArgument((preds.size() == ichans.size()) || (preds.isEmpty() && ichans.size() == 1));
			if (preds.isEmpty()) {
				checkArgument(!overallInput, "two overall inputs?!");
				Channel<?> chan = Iterables.get(ichans, 0, null);
				retval.add(new IOInfo(null, w, chan, Blob.Token.createOverallInputToken(w), ConnectionKind.INPUT));
				overallInput = true;
			}
			for (int i = 0; i < preds.size(); ++i) {
				Worker<?, ?> pred = preds.get(i);
				Channel<?> chan = Iterables.get(ichans, i, null);
				Blob.Token token = new Blob.Token(pred, w);
				retval.add(new IOInfo(pred, w, chan, token,
						workers.contains(pred) ? ConnectionKind.INTERNAL : ConnectionKind.INPUT));
			}
		}
		for (Worker<?, ?> w : workers) {
			List<Worker<?, ?>> succs = (List<Worker<?, ?>>)Workers.getSuccessors(w);
			List<Channel<?>> ochans = (List<Channel<?>>)Workers.getOutputChannels(w);
			checkArgument((succs.size() == ochans.size()) || (succs.isEmpty() && ochans.size() == 1));
			if (succs.isEmpty()) {
				checkArgument(!overallOutput, "two overall outputs?!");
				Channel<?> chan = Iterables.get(ochans, 0, null);
				retval.add(new IOInfo(w, null, chan, Blob.Token.createOverallOutputToken(w), ConnectionKind.OUTPUT));
				overallOutput = true;
			}
			for (int i = 0; i < succs.size(); ++i) {
				Worker<?, ?> succ = succs.get(i);
				Channel<?> chan = Iterables.get(ochans, i, null);
				Blob.Token token = new Blob.Token(w, succ);
				retval.add(new IOInfo(w, succ, chan, token,
						workers.contains(succ) ? ConnectionKind.INTERNAL : ConnectionKind.OUTPUT));
			}
		}
		return retval.build();
	}

	/**
	 * Creates IOInfo objects for all external edges of the given set of workers
	 * (that is, edges where exactly one worker is in the set).  The workers
	 * must have their predecessor/successor relationships initialized, but they
	 * need not be connected with channels.  The given set need not be
	 * connected (in the reachability sense).
	 * @param workers a set of workers
	 * @return a set of IOInfo objects for all external edges of the given set
	 */
	public static ImmutableSet<IOInfo> externalEdges(Set<Worker<?, ?>> workers) {
		return FluentIterable.from(allEdges(workers)).filter(new Predicate<IOInfo>() {
			@Override
			public boolean apply(IOInfo input) {
				return input.isInput() || input.isOutput();
			}
		}).toSet();
	}

	/**
	 * Creates IOInfo objects for all internal edges of the given set of workers
	 * (that is, edges where both workers are in the set).  The workers
	 * must have their predecessor/successor relationships initialized, but they
	 * need not be connected with channels.  The given set need not be
	 * connected (in the reachability sense).
	 * @param workers a set of workers
	 * @return a set of IOInfo objects for all internal edges of the given set
	 */
	public static ImmutableSet<IOInfo> internalEdges(Set<Worker<?, ?>> workers) {
		return FluentIterable.from(allEdges(workers)).filter(new Predicate<IOInfo>() {
			@Override
			public boolean apply(IOInfo input) {
				return input.connectionKind == ConnectionKind.INTERNAL;
			}
		}).toSet();
	}

	public Worker<?, ?> upstream() {
		return upstream;
	}

	public Worker<?, ?> downstream() {
		return downstream;
	}

	//TODO: is this the correct place for these methods?
	//We often need this for intra-blob checks too.
	public int getUpstreamChannelIndex() {
		if (token().isOverallOutput())
			return 0;
		return Workers.getSuccessors(upstream).indexOf(downstream);
	}

	public int getDownstreamChannelIndex() {
		if (token().isOverallInput())
			return 0;
		return Workers.getPredecessors(downstream).indexOf(upstream);
	}

	public Channel<?> channel() {
		return channel;
	}

	public Blob.Token token() {
		return token;
	}

	public boolean isInput() {
		return connectionKind.equals(ConnectionKind.INPUT);
	}

	public boolean isOutput() {
		return connectionKind.equals(ConnectionKind.OUTPUT);
	}

	@Override
	public String toString() {
		return String.format("%s %s: %s -> %s, %s",
				connectionKind.toString().toLowerCase(Locale.ENGLISH),
				token(),
				upstream(),
				downstream(),
				channel());
	}

	/**
	 * Orders IOInfo by the natural ordering of the contained Token.  This
	 * Comparator is inconsistent with equals.
	 *
	 * IOInfo doesn't implement Comparable directly because its natural ordering
	 * would be inconsistent with equals.
	 */
	public static final Comparator<IOInfo> TOKEN_SORT = new Comparator<IOInfo>() {
		@Override
		public int compare(IOInfo o1, IOInfo o2) {
			return o1.token().compareTo(o2.token());
		}
	};
}
