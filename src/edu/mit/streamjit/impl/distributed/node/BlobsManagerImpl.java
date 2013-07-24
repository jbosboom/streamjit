package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BlobExecuter;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	private Set<BlobExecuter> blobExecuters;

	public BlobsManagerImpl(Set<Blob> blobSet, Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap, Map<Token, Integer> portIdMap,
			Map<Integer, NodeInfo> nodeInfoMap) {
		blobExecuters = new HashSet<>();
		for (Blob b : blobSet)
			blobExecuters.add(new BlobExecuter(b, tokenMachineMap, portIdMap, nodeInfoMap));
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
	
	private class BlobExecuter {

		Blob blob;
		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap;
		Map<Token, Integer> portIdMap;
		Map<Integer, NodeInfo> nodeInfoMap;

		List<Thread> blobThreads;

		List<BoundaryInputChannel> inputChannels;
		List<BoundaryOutputChannel> outputChannels;

		private BlobExecuter(Blob blob,
				Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap,
				Map<Token, Integer> portIdMap, Map<Integer, NodeInfo> nodeInfoMap) {
			this.blob = blob;
			this.blobThreads = new ArrayList<>();
			this.inputChannels = new ArrayList<>();
			this.outputChannels = new ArrayList<>();
			this.tokenMachineMap = tokenMachineMap;
			this.portIdMap = portIdMap;
			this.nodeInfoMap = nodeInfoMap;

			initialize();
		}

		private void initialize() {
			for (int i = 0; i < blob.getCoreCount(); i++) {
				blobThreads.add(new Thread(blob.getCoreCode(i)));
			}

			Set<Token> inputTokens = blob.getInputs();
			for (Token t : inputTokens) {
				int portNo = getPortNo(t);
				String ipAddress = getIPAddress(t);
				inputChannels.add(new TCPInputChannel<>(
						inputTokenChannelMap.get(t), ipAddress, portNo));
			}

			Map<Token, Channel<?>> outputTokenChannelMap = blob.getOutputChannels();
			for (Token t : outputTokenChannelMap.keySet()) {
				int portNo = getPortNo(t);
				outputChannels.add(new TCPOutputChannel<>(outputTokenChannelMap
						.get(t), portNo));
			}
		}

		private String getIPAddress(Token t) {
			if (!tokenMachineMap.containsKey(t))
				throw new IllegalArgumentException(t
						+ " is not found in the tokenMachineMap");

			int machineID = tokenMachineMap.get(t).getKey();
			NodeInfo nodeInfo = nodeInfoMap.get(machineID);
			return nodeInfo.getIpAddress().getHostAddress();
		}

		private int getPortNo(Token t) {
			if (!portIdMap.containsKey(t))
				throw new IllegalArgumentException(t
						+ " is not found in the portIdMap");
			return portIdMap.get(t);
		}

		private void start() {

			for (BoundaryChannel<?> bc : inputChannels) {
				new Thread(bc.getRunnable()).start();
			}

			for (BoundaryChannel<?> bc : outputChannels) {
				new Thread(bc.getRunnable()).start();
			}

			for (Thread t : blobThreads)
				t.start();
		}

		private void stop() {

			for (BoundaryChannel<?> bc : inputChannels) {
				bc.stop();
			}

			for (BoundaryChannel<?> bc : outputChannels) {
				bc.stop();
			}

			doDrain();

			for (Thread t : blobThreads)
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		private void doDrain() {
			// We are passing null callback here as DistributedBlob can handle the
			// draining within it's sub singlethreadedblobs. We may
			// change this later and perform the draining in a global and ordered
			// manner.
			this.blob.drain(null);
		}
	}
}
