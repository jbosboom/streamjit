package edu.mit.streamjit.impl.concurrent;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;

/**
 * This {@link ChannelFactory} manufactures {@link ArrayChannel}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public class ConcurrentChannelFactory implements ChannelFactory {
	@Override
	public <E> Channel<E> makeChannel(Worker<?, E> upstream,
			Worker<E, ?> downstream) {
		return new ArrayChannel<E>();
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof ConcurrentChannelFactory;
	}

	@Override
	public int hashCode() {
		return 0;
	}
}