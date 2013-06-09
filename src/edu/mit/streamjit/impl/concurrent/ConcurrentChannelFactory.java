package edu.mit.streamjit.impl.concurrent;

import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.SynchronizedChannel;

/**
 * This {@link ChannelFactory} manufactures {@link Channel}s based on the existence of the {@link Worker}s on the {@link Blob}. Returns
 * {@link ArrayChannel} if both upstream and downstream are happened to exist in the same blob.Returns {@link SynchronizedChannel}
 * otherwise.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public class ConcurrentChannelFactory implements ChannelFactory {

	Set<Worker<?, ?>> blobWorkers;

	public ConcurrentChannelFactory(Set<Worker<?, ?>> blobWorkers) {
		this.blobWorkers = blobWorkers;
	}

	@Override
	public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
		assert blobWorkers.contains(upstream) || blobWorkers.contains(downstream) : "Illegal assignment: source worker is not in the current blob";
		Channel<E> chnl;

		if (blobWorkers.contains(upstream) && blobWorkers.contains(downstream))
			chnl = new ArrayChannel<E>();
		else
			chnl = new SynchronizedChannel<>(new ArrayChannel<E>());
		return chnl;
	}
}
