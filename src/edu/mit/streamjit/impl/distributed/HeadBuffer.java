package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BlobGraph.AbstractDrainer;

public class HeadBuffer extends AbstractReadOnlyBuffer {

	Buffer buffer;
	AbstractDrainer drainer;

	HeadBuffer(Buffer buffer, AbstractDrainer drainer) {
		this.buffer = buffer;
		this.drainer = drainer;
	}

	// TODO: Need to optimise the buffer reading. I will come back here later.
	@Override
	public Object read() {
		Object o = buffer.read();
		if (buffer.size() == 0) {
			new DrainerThread().start();
		}
		return o;
	}
	@Override
	public int size() {
		return buffer.size();
	}

	class DrainerThread extends Thread {
		DrainerThread() {
			super("DrainerThread");
		}

		public void run() {
			drainer.startDraining(true);
		}
	}
}