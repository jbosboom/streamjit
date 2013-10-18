package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.common.SystemInfo.SystemInfoProcessor;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;

public class SystemInfoProcessorImpl implements SystemInfoProcessor {

	StreamNodeAgent streamNode;

	public SystemInfoProcessorImpl(StreamNodeAgent streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(SystemInfo systemInfo) {
		streamNode.setSystemInfo(systemInfo);
	}
}
