package edu.mit.streamjit.impl.distributed;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.TCPOutputChannel;

/**
 * Head Channel is just a wrapper to TCPOutputChannel that skips
 * fillUnprocessedData.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 21, 2013
 */
public class HeadChannel extends TCPOutputChannel {

	public HeadChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
	}

	protected void fillUnprocessedData() {
		this.unProcessedData = ImmutableList.of();
	}

	/**
	 * Head HeadBuffer is just a wrapper to to a buffer that triggers final
	 * draining process if it finds out that there is no more data in the
	 * buffer. This is need for non manual inputs.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Oct 2, 2013
	 */
	public static class HeadBuffer extends AbstractReadOnlyBuffer {

		Buffer buffer;
		AbstractDrainer drainer;

		public HeadBuffer(Buffer buffer, AbstractDrainer drainer) {
			this.buffer = buffer;
			this.drainer = drainer;
		}

		// TODO: Need to optimise the buffer reading. I will come back here
		// later.
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
				System.out.println("Input data finished");
				drainer.startDraining(2);
			}
		}
	}
}