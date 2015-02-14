package edu.mit.streamjit.impl.distributed;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.AsyncOutputChannel;
import edu.mit.streamjit.impl.distributed.node.BlockingOutputChannel;

/**
 * Head Channel is just a wrapper to TCPOutputChannel that skips
 * fillUnprocessedData.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 21, 2013
 */
public class HeadChannel {

	public static class TCPHeadChannel extends BlockingOutputChannel {

		public TCPHeadChannel(Buffer buffer, ConnectionProvider conProvider,
				ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugLevel);
		}

		protected void fillUnprocessedData() {
			this.unProcessedData = ImmutableList.of();
		}
	}

	public static class AsyncHeadChannel extends AsyncOutputChannel {

		final Buffer readBuffer;
		private volatile boolean stopCalled;
		private volatile boolean isFinal;

		public AsyncHeadChannel(Buffer buffer, ConnectionProvider conProvider,
				ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
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
					int written = 0;
					while (!stopCalled) {
						read = readBuffer.read(data, 0, data.length);
						written = 0;
						while (written < read) {
							written += writeBuffer.write(data, written, read
									- written);
							if (written == 0) {
								try {
									// TODO: Verify this sleep time.
									Thread.sleep(500);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					}
					stopSuper(isFinal);
				}
			};
		}

		protected void fillUnprocessedData() {
			throw new Error("Method not implemented");
		}

		@Override
		public void stop(boolean isFinal) {
			this.isFinal = isFinal;
			this.stopCalled = true;
		}

		private void stopSuper(boolean isFinal) {
			super.stop(isFinal);
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
				// drainer.startDraining(2);
				drainer.drainFinal(false);
			}
		}
	}
}