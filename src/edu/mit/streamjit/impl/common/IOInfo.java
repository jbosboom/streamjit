package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.interp.Channel;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public class IOInfo {
	private final Worker<?, ?> upstream;
	private final Worker<?, ?> downstream;
	private final Channel<?> channel;
	private final Blob.Token token;
	private final boolean isInput;

	private IOInfo(Worker<?, ?> upstream, Worker<?, ?> downstream, Channel<?> channel, Blob.Token token, boolean isInput) {
		checkArgument(upstream != null || downstream != null);
		checkArgument(isInput && downstream != null || !isInput && upstream != null);
		if (upstream != null && downstream != null)
			checkArgument(Workers.compareStreamPosition(upstream, downstream) == Workers.StreamPosition.UPSTREAM);
		this.upstream = upstream;
		this.downstream = downstream;
		this.channel = channel;
		this.token = checkNotNull(token);
		this.isInput = isInput;
	}

	@SuppressWarnings(value = "unchecked")
	public static ImmutableSet<IOInfo> create(Set<Worker<?, ?>> workers) {
		ImmutableSet.Builder<IOInfo> retval = ImmutableSet.builder();
		boolean overallInput = false;
		boolean overallOutput = false;
		for (Worker<?, ?> w : workers) {
			List<Worker<?, ?>> preds = (List<Worker<?, ?>>)(List)Workers.getPredecessors(w);
			List<Channel<?>> ichans = (List<Channel<?>>)(List)Workers.getInputChannels(w);
			checkArgument((preds.size() == ichans.size()) || (preds.isEmpty() && ichans.size() == 1));
			if (preds.isEmpty()) {
				checkArgument(!overallInput, "two overall inputs?!");
				retval.add(new IOInfo(null, w, ichans.get(0), Blob.Token.createOverallInputToken(w), true));
				overallInput = true;
			}
			for (int i = 0; i < preds.size(); ++i) {
				Worker<?, ?> pred = preds.get(i);
				if (workers.contains(pred)) continue;
				Channel<?> chan = Iterables.get(ichans, i, null);
				Blob.Token token = new Blob.Token(pred, w);
				retval.add(new IOInfo(pred, w, chan, token, true));
			}
		}
		for (Worker<?, ?> w : workers) {
			List<Worker<?, ?>> succs = (List<Worker<?, ?>>)(List)Workers.getSuccessors(w);
			List<Channel<?>> ochans = (List<Channel<?>>)(List)Workers.getOutputChannels(w);
			checkArgument((succs.size() == ochans.size()) || (succs.isEmpty() && ochans.size() == 1));
			if (succs.isEmpty()) {
				checkArgument(!overallOutput, "two overall outputs?!");
				retval.add(new IOInfo(w, null, ochans.get(0), Blob.Token.createOverallOutputToken(w), false));
				overallOutput = true;
			}
			for (int i = 0; i < succs.size(); ++i) {
				Worker<?, ?> succ = succs.get(i);
				if (workers.contains(succ)) continue;
				Channel<?> chan = Iterables.get(ochans, i, null);
				Blob.Token token = new Blob.Token(w, succ);
				retval.add(new IOInfo(w, succ, chan, token, false));
			}
		}
		return retval.build();
	}

	public Worker<?, ?> upstream() {
		return upstream;
	}

	public Worker<?, ?> downstream() {
		return downstream;
	}

	public Channel<?> channel() {
		return channel;
	}

	public Blob.Token token() {
		return token;
	}

	public boolean isInput() {
		return isInput;
	}

	public boolean isOutput() {
		return !isInput();
	}

	@Override
	public String toString() {
		return String.format("%s %s: %s -> %s, %s",
				isInput() ? "input" : "output",
				token(),
				upstream(),
				downstream(),
				channel());
	}
}
