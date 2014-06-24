package edu.mit.streamjit.impl.distributed;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.AsyncTCPOutputChannel;
import edu.mit.streamjit.impl.distributed.node.TCPOutputChannel;

/**
 * Head Channel is just a wrapper to TCPOutputChannel that skips
 * fillUnprocessedData.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 21, 2013
 */
public class HeadChannel {

	public static class TCPHeadChannel extends TCPOutputChannel {

		public TCPHeadChannel(Buffer buffer, TCPConnectionProvider conProvider,
				ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
		}

		protected void fillUnprocessedData() {
			this.unProcessedData = ImmutableList.of();
		}
	}

	public static class AsyncTCPHeadChannel extends AsyncTCPOutputChannel {

		final Buffer readBuffer;
		private volatile boolean stopCalled;
		public AsyncTCPHeadChannel(Buffer buffer,
				TCPConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel) {
			super(conProvider, conInfo, bufferTokenName, debugLevel);
			readBuffer = buffer;
			stopCalled = false;
		}

		@Override
		public Runnable getRunnable() {
			final Runnable supperRunnable = super.getRunnable();
			return new Runnable() {
				@Override
				public void run() {
					supperRunnable.run();
					final Buffer writeBuffer = getBuffer();
					final int dataLength = 10000;
					final Object[] data = new Object[dataLength];
					int read = 1;
					while (read != 0 && !stopCalled) {
						read = readBuffer.read(data, 0, data.length);
						writeBuffer.write(data, 0, read);
					}
				}
			};
		}
		protected void fillUnprocessedData() {
			throw new Error("Method not implemented");
		}

		@Override
		public void stop(boolean isFinal) {
			super.stop(isFinal);
			this.stopCalled = true;
		}
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
		public int read(Object[] data, int offset, int length) {
			int read = buffer.read(data, offset, length);
			if (read == 0) {
				new DrainerThread().start();
			}
			return read;
		}

		@Override
		public boolean readAll(Object[] data) {
			return buffer.readAll(data);
		}

		@Override
		public boolean readAll(Object[] data, int offset) {
			return buffer.readAll(data, offset);
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