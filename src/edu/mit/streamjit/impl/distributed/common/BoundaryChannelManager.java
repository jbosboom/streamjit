package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.*;

/**
 * Manages set of {@link BoundaryChannel}s.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2014
 */
public interface BoundaryChannelManager {

	void start();

	void waitToStart();

	void waitToStop();

	public interface BoundaryInputChannelManager extends BoundaryChannelManager {

		/**
		 * In streamJit, a channel can be identified by a {@link Token}.
		 * 
		 * @return map of channel {@link Token}, {@link BoundaryInputChannel}
		 *         handled by this manager.
		 */
		ImmutableMap<Token, BoundaryInputChannel> inputChannelsMap();

		/**
		 * @param stopType
		 *            See {@link BoundaryInputChannel#stop(int)}
		 */
		void stop(int stopType);
	}

	public interface BoundaryOutputChannelManager
			extends
				BoundaryChannelManager {

		/**
		 * In streamJit, a channel can be identified by a {@link Token}.
		 * 
		 * @return map of channel {@link Token}, {@link BoundaryOutputChannel}
		 *         handled by this manager.
		 */
		ImmutableMap<Token, BoundaryOutputChannel> outputChannelsMap();

		/**
		 * @param stopType
		 *            See {@link BoundaryOutputChannel#stop(boolean)}
		 */
		void stop(boolean stopType);
	}
}
