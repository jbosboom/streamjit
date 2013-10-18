package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo.NodeInfoProcessor;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;

/**
 * {@link NodeInfoProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class NodeInfoProcessorImpl implements NodeInfoProcessor {

	StreamNodeAgent streamNode;

	public NodeInfoProcessorImpl(StreamNodeAgent streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(NodeInfo nodeInfo) {
		streamNode.setNodeInfo(nodeInfo);
	}
}
