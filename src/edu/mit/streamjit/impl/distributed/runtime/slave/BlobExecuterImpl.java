package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobExecuter;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobExecuterImpl implements BlobExecuter {

	Blob blob;
	Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap;
	Map<Token, Integer> portIdMap;
	Map<Integer, NodeInfo> nodeInfoMap;

	List<Thread> blobThreads;

	List<BoundaryInputChannel<?>> inputChannels;
	List<BoundaryOutputChannel<?>> outputChannels;

	public BlobExecuterImpl(Blob blob, Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap, Map<Token, Integer> portIdMap,
			Map<Integer, NodeInfo> nodeInfoMap) {
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

		Map<Token, Channel<?>> inputTokenChannelMap = blob.getInputChannels();
		for (Token t : inputTokenChannelMap.keySet()) {
			int portNo = getPortNo(t);
			String ipAddress = getIPAddress(t);
			inputChannels.add(new TCPInputChannel<>(inputTokenChannelMap.get(t), ipAddress, portNo));
		}

		Map<Token, Channel<?>> outputTokenChannelMap = blob.getOutputChannels();
		for (Token t : outputTokenChannelMap.keySet()) {
			int portNo = getPortNo(t);
			outputChannels.add(new TCPOutputChannel<>(outputTokenChannelMap.get(t), portNo));
		}
	}

	private String getIPAddress(Token t) {
		if (!tokenMachineMap.containsKey(t))
			throw new IllegalArgumentException(t + " is not found in the tokenMachineMap");

		int machineID = tokenMachineMap.get(t).getKey();
		NodeInfo nodeInfo = nodeInfoMap.get(machineID);
		return nodeInfo.getIpAddress().getHostAddress();
	}

	private int getPortNo(Token t) {
		if (!portIdMap.containsKey(t))
			throw new IllegalArgumentException(t + " is not found in the portIdMap");
		return portIdMap.get(t);
	}

	@Override
	public void start() {

		for (BoundaryChannel<?> bc : inputChannels) {
			new Thread(bc.getRunnable()).start();
		}

		for (BoundaryChannel<?> bc : outputChannels) {
			new Thread(bc.getRunnable()).start();
		}

		for (Thread t : blobThreads)
			t.start();
	}

	@Override
	public void stop() {

		for (BoundaryChannel<?> bc : inputChannels) {
			bc.stop();
		}

		for (BoundaryChannel<?> bc : outputChannels) {
			bc.stop();
		}

		for (Thread t : blobThreads)
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	@Override
	public void suspend() {
	}

	@Override
	public void resume() {
	}
}
