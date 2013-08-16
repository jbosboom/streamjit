package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo.NodeInfoProcessor;

/**
 * {@link NodeInfoProcessor} at {@link StreamNode} side.
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class SNNodeInfoProcessorImpl implements NodeInfoProcessor {

	@Override
	public void process(NodeInfo nodeInfo) {
		throw new AssertionError(
				"NodeInfo doesn't support MessageVisitor for the moment.");
	}
}
