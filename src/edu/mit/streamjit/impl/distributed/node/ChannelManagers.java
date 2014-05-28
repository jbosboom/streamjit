package edu.mit.streamjit.impl.distributed.node;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryInputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryOutputChannelManager;

public class ChannelManagers {

	public static class BlockingInputChannelManager
			implements
				BoundaryInputChannelManager {

		private final ImmutableMap<Token, BoundaryInputChannel> inputChannels;

		private final Set<Thread> inputChannelThreads;

		public BlockingInputChannelManager(
				final ImmutableMap<Token, BoundaryInputChannel> inputChannels) {
			this.inputChannels = inputChannels;
			inputChannelThreads = new HashSet<>(inputChannels.values().size());
		}

		@Override
		public void start() {
			for (BoundaryInputChannel bc : inputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				inputChannelThreads.add(t);
			}
		}

		@Override
		public void waitToStart() {
		}

		@Override
		public void waitToStop() {
			for (Thread t : inputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void stop(int stopType) {
			for (BoundaryInputChannel bc : inputChannels.values()) {
				bc.stop(stopType);
			}
		}

		@Override
		public ImmutableMap<Token, BoundaryInputChannel> inputChannelsMap() {
			return inputChannels;
		}
	}

	public static class BlockingOutputChannelManager
			implements
				BoundaryOutputChannelManager {

		protected final ImmutableMap<Token, BoundaryOutputChannel> outputChannels;
		protected final Set<Thread> outputChannelThreads;

		public BlockingOutputChannelManager(
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			this.outputChannels = outputChannels;
			outputChannelThreads = new HashSet<>(outputChannels.values().size());
		}

		@Override
		public void start() {
			for (BoundaryOutputChannel bc : outputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				outputChannelThreads.add(t);
			}
		}

		@Override
		public void waitToStart() {
		}

		@Override
		public void stop(boolean stopType) {
			for (BoundaryOutputChannel bc : outputChannels.values()) {
				bc.stop(stopType);
			}
		}

		@Override
		public void waitToStop() {
			for (Thread t : outputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public ImmutableMap<Token, BoundaryOutputChannel> outputChannelsMap() {
			return outputChannels;
		}
	}

	public static class AsynchronousOutputChannelManager
			extends
				BlockingOutputChannelManager {

		public AsynchronousOutputChannelManager(
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			super(outputChannels);
		}

		@Override
		public void waitToStart() {
			for (Thread t : outputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
