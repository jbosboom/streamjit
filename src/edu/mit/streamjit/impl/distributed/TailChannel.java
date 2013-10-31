package edu.mit.streamjit.impl.distributed;

import java.util.concurrent.CountDownLatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.TCPInputChannel;

public class TailChannel extends TCPInputChannel {

	int limit;

	int count;

	CountDownLatch latch;

	public TailChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugPrint,
			int limit) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugPrint);
		this.limit = limit;
		count = 0;
		latch = new CountDownLatch(1);
	}

	@Override
	public void receiveData() {
		super.receiveData();
		count++;
		if (count == limit)
			latch.countDown();
	}

	public void awaitForFixInput() throws InterruptedException {
		latch.await();
	}

	public void reset() {
		latch.countDown();
		latch = new CountDownLatch(1);
		count = 0;
	}
}