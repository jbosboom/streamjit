/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobExecuter;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobsManager;
import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;

public class BlobsManagerImpl implements BlobsManager {

	Set<BlobExecuter> blobExecuters;

	public BlobsManagerImpl(Set<Blob> blobSet, Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap, Map<Token, Integer> portIdMap,
			Map<Integer, NodeInfo> nodeInfoMap) {
		blobExecuters = new HashSet<>();
		for (Blob b : blobSet)
			blobExecuters.add(new BlobExecuterImpl(b, tokenMachineMap, portIdMap, nodeInfoMap));
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

	@Override
	public void suspend() {

		for (BlobExecuter be : blobExecuters)
			be.suspend();
	}

	@Override
	public void resume() {
		for (BlobExecuter be : blobExecuters)
			be.resume();
	}
}
