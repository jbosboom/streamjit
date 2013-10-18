package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;

/**
 * {@link RequestProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class RequestProcessorImpl implements RequestProcessor {

	StreamNode streamNode;

	RequestProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void processAPPStatus() {
		System.out.println("APPStatus requested");
	}

	@Override
	public void processSysInfo() {
		System.out.println("SysInfo requested");
	}

	@Override
	public void processMachineID() {
		try {
			Integer id = streamNode.controllerConnection.readObject();
			streamNode.setNodeID(id);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processNodeInfo() {
		NodeInfo myInfo = NodeInfo.getMyinfo();
		try {
			streamNode.controllerConnection.writeObject(myInfo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
