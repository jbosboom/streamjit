package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.DrainElement;
import edu.mit.streamjit.impl.distributed.common.MessageElement;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	private Set<BlobExecuter> blobExecuters;

	private Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap;
	private Map<Token, Integer> portIdMap;
	private Map<Integer, NodeInfo> nodeInfoMap;
	private final StreamNode streamNode;
	private final TCPConnectionProvider conProvider;
	private Map<Token, TCPConnectionInfo> conInfoMap;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap,
			Map<Token, Integer> portIdMap, Map<Integer, NodeInfo> nodeInfoMap,
			Map<Token, TCPConnectionInfo> conInfoMap, StreamNode streamNode,
			TCPConnectionProvider conProvider) {

		this.tokenMachineMap = tokenMachineMap;
		this.portIdMap = portIdMap;
		this.nodeInfoMap = nodeInfoMap;
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;

		ImmutableMap<Token, Buffer> bufferMap = createBufferMap(blobSet);

		for (Blob b : blobSet) {
			b.installBuffers(bufferMap);
		}

		Set<Token> locaTokens = getLocalTokens(blobSet);
		blobExecuters = new HashSet<>();
		for (Blob b : blobSet) {
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = createInputChannels(
					Sets.difference(b.getInputs(), locaTokens), bufferMap);
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = createOutputChannels(
					Sets.difference(b.getOutputs(), locaTokens), bufferMap);
			blobExecuters
					.add(new BlobExecuter(b, inputChannels, outputChannels));
		}
	}

	@Override
	public void start() {
		for (BlobExecuter be : blobExecuters)
			be.start();
	}

	@Override
	public void stop() {
		for (BlobExecuter be : blobExecuters)
			be.stop();
	}

	// TODO: Buffer sizes, including head and tail buffers, must be optimized.
	// consider adding some tuning factor
	private ImmutableMap<Token, Buffer> createBufferMap(Set<Blob> blobSet) {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
		Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

		for (Blob b : blobSet) {
			Set<Blob.Token> inputs = b.getInputs();
			for (Token t : inputs) {
				minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}

			Set<Blob.Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}
		}

		Set<Token> localTokens = Sets.intersection(minInputBufCapaciy.keySet(),
				minOutputBufCapaciy.keySet());
		Set<Token> globalInputTokens = Sets.difference(
				minInputBufCapaciy.keySet(), localTokens);
		Set<Token> globalOutputTokens = Sets.difference(
				minOutputBufCapaciy.keySet(), localTokens);

		for (Token t : localTokens) {
			int bufSize;
			bufSize = lcm(minInputBufCapaciy.get(t), minOutputBufCapaciy.get(t));
			// TODO: Just to increase the performance. Change it later
			bufSize = Math.max(1000, bufSize);
			Buffer buf = new ConcurrentArrayBuffer(bufSize);
			bufferMapBuilder.put(t, buf);
		}

		for (Token t : Sets.union(globalInputTokens, globalOutputTokens)) {
			bufferMapBuilder.put(t, new ConcurrentArrayBuffer(1000));
		}

		return bufferMapBuilder.build();
	}

	private int gcd(int a, int b) {
		while (true) {
			if (a == 0)
				return b;
			b %= a;
			if (b == 0)
				return a;
			a %= b;
		}
	}

	private int lcm(int a, int b) {
		int val = gcd(a, b);
		return val != 0 ? ((a * b) / val) : 0;
	}

	private Set<Token> getLocalTokens(Set<Blob> blobSet) {
		Set<Token> inputTokens = new HashSet<>();
		Set<Token> outputTokens = new HashSet<>();

		for (Blob b : blobSet) {
			Set<Token> inputs = b.getInputs();
			for (Token t : inputs) {
				inputTokens.add(t);
			}

			Set<Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				outputTokens.add(t);
			}
		}
		return Sets.intersection(inputTokens, outputTokens);
	}

	private ImmutableMap<Token, BoundaryInputChannel> createInputChannels(
			Set<Token> inputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryInputChannel> inputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : inputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			inputChannelMap.put(t, new TCPInputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), false));
		}
		return inputChannelMap.build();
	}

	private ImmutableMap<Token, BoundaryOutputChannel> createOutputChannels(
			Set<Token> outputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryOutputChannel> outputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : outputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			outputChannelMap.put(t, new TCPOutputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), false));
		}
		return outputChannelMap.build();
	}
	private String getIPAddress(Token t) {
		if (!tokenMachineMap.containsKey(t))
			throw new IllegalArgumentException(t
					+ " is not found in the tokenMachineMap");

		int machineID = tokenMachineMap.get(t).getKey();
		NodeInfo nodeInfo = nodeInfoMap.get(machineID);
		return nodeInfo.getIpAddress().getHostAddress();
	}

	private class BlobExecuter {

		private volatile boolean isDrained;
		private final Token blobID;

		private final Blob blob;
		private Set<BlobThread> blobThreads;

		Set<BoundaryInputChannel> inputChannels;
		Set<BoundaryOutputChannel> outputChannels;

		Set<Thread> inputChannelThreads;
		Set<Thread> outputChannelThreads;

		private boolean reqDrainData;

		private BlobExecuter(Blob blob,
				ImmutableMap<Token, BoundaryInputChannel> inputChannels,
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			this.blob = blob;
			this.blobThreads = new HashSet<>();
			assert blob.getInputs().containsAll(inputChannels.keySet());
			assert blob.getOutputs().containsAll(outputChannels.keySet());
			this.inputChannels = new HashSet<>(inputChannels.values());
			this.outputChannels = new HashSet<>(outputChannels.values());
			inputChannelThreads = new HashSet<>(inputChannels.values().size());
			outputChannelThreads = new HashSet<>(outputChannels.values().size());

			for (int i = 0; i < blob.getCoreCount(); i++) {
				blobThreads.add(new BlobThread(blob.getCoreCode(i)));
			}

			isDrained = false;
			this.blobID = Utils.getBlobID(blob);
		}

		private void start() {
			for (BoundaryInputChannel bc : inputChannels) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				inputChannelThreads.add(t);
			}

			for (BoundaryOutputChannel bc : outputChannels) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				outputChannelThreads.add(t);
			}

			for (Thread t : blobThreads)
				t.start();
		}

		private void stop() {

			for (BoundaryInputChannel bc : inputChannels) {
				bc.stop();
			}

			for (BoundaryOutputChannel bc : outputChannels) {
				bc.stop(true);
			}

			for (Thread t : blobThreads)
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		private void doDrain(boolean reqDrainData) {
			this.reqDrainData = reqDrainData;

			for (BoundaryInputChannel bc : inputChannels) {
				bc.stop();
			}

			for (Thread t : inputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			DrainCallback dcb = new DrainCallback(this);
			this.blob.drain(dcb);
		}

		private void drained() {
			for (BlobThread bt : blobThreads) {
				bt.requestStop();
			}

			for (BoundaryOutputChannel bc : outputChannels) {
				bc.stop(true);
			}

			for (Thread t : outputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			this.isDrained = true;
			MessageElement drained = new DrainElement.Drained(blobID);
			try {
				streamNode.controllerConnection.writeObject(drained);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (this.reqDrainData) {
				ImmutableMap.Builder<Token, DrainData> builder = new ImmutableMap.Builder<>();
				builder.put(blobID, blob.getDrainData());
				MessageElement me = new DrainElement.DrainedDataMap(
						builder.build());
				try {
					streamNode.controllerConnection.writeObject(me);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		public Token getBlobID() {
			return Utils.getBlobID(blob);
		}
	}

	private static class DrainCallback implements Runnable {

		private final BlobExecuter blobExec;

		DrainCallback(BlobExecuter be) {
			this.blobExec = be;
		}

		@Override
		public void run() {
			blobExec.drained();
		}
	}

	@Override
	public void drain(Token blobID, boolean reqDrainData) {
		for (BlobExecuter be : blobExecuters) {
			if (be.getBlobID().equals(blobID)) {
				be.doDrain(reqDrainData);
				break;
			}
		}
	}

	@Override
	public void reqDrainedData(Set<Token> blobSet) {
		ImmutableMap.Builder<Token, DrainData> builder = new ImmutableMap.Builder<>();
		for (BlobExecuter be : blobExecuters) {
			if (be.isDrained) {
				builder.put(be.blobID, be.blob.getDrainData());
			}
		}

		try {
			streamNode.controllerConnection
					.writeObject(new DrainElement.DrainedDataMap(builder
							.build()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
