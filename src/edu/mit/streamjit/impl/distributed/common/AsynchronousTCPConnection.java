package edu.mit.streamjit.impl.distributed.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsynchronousTCPConnection implements Connection {
	private ObjectOutputStream ooStream = null;
	private AsynchronousSocketChannel asyncSktChannel;

	private MyByteArrayOutputStream bAos;
	private AtomicBoolean canWrite;

	private boolean isconnected = false;
	private final int resetCount;

	public AsynchronousTCPConnection(AsynchronousSocketChannel asyncSktChannel) {
		this(asyncSktChannel, 5000);
	}

	/**
	 * @param socket
	 * @param resetCount
	 *            reset the {@link ObjectOutputStream} after this no of sends.
	 *            To avoid out of memory error.
	 */
	public AsynchronousTCPConnection(AsynchronousSocketChannel asyncSktChannel,
			int resetCount) {
		this.resetCount = resetCount;
		try {
			this.asyncSktChannel = asyncSktChannel;

			bAos = new MyByteArrayOutputStream(4096);
			ooStream = new ObjectOutputStream(bAos);
			isconnected = true;
			canWrite = new AtomicBoolean(true);
			// System.out.println(String.format(
			// "DEBUG: TCP connection %d has been established", count++));
		} catch (IOException iex) {
			isconnected = false;
			iex.printStackTrace();
		}
	}

	// This is introduced to reduce the ooStream.reset(); frequency. Too many
	// resets, i.e., reset the ooStream for every new write severely affects the
	// performance.
	int n = 0;

	@Override
	public void writeObject(Object obj) throws IOException {
		if (isStillConnected()) {
			try {
				ooStream.writeObject(obj);
				ByteBuffer bb = ByteBuffer.wrap(bAos.getBuf(), 0,
						bAos.getCount());

				Future<Integer> nBytes = asyncSktChannel.write(bb);
				bAos.reset();

				n++;
				// TODO: Any way to improve the performance?
				if (n > resetCount) {
					n = 0;
					ooStream.reset();
				}
				// System.out.println("Object send...");
			} catch (IOException ix) {
				// Following doesn't change when other side of the socket is
				// closed.....
				/*
				 * System.out.println("socket.isBound()" + socket.isBound());
				 * System.out.println("socket.isClosed()" + socket.isClosed());
				 * System.out.println("socket.isConnected()" +
				 * socket.isConnected());
				 * System.out.println("socket.isInputShutdown()" +
				 * socket.isInputShutdown());
				 * System.out.println("socket.isOutputShutdown()" +
				 * socket.isOutputShutdown());
				 */
				isconnected = false;
				throw ix;
			}
		} else {
			throw new IOException("TCPConnection: Socket is not connected");
		}
	}

	public final void closeConnection() {
		try {
			if (ooStream != null)
				this.ooStream.close();
			if (asyncSktChannel != null)
				this.asyncSktChannel.close();
		} catch (IOException ex) {
			isconnected = false;
			ex.printStackTrace();
		}
	}

	@Override
	public final boolean isStillConnected() {
		// return (this.socket.isConnected() && !this.socket.isClosed());
		return isconnected;
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		throw new IOException(
				"Reading object is not supported in asynchronous tcp mode");
	}

	public InetAddress getInetAddress() {
		throw new java.lang.Error("Method not Implemented");
	}

	@Override
	public void softClose() throws IOException {
		this.ooStream.write('\u001a');
		this.ooStream.flush();
	}

	// http://stackoverflow.com/a/15686667/505406
	private class MyByteArrayOutputStream extends ByteArrayOutputStream {
		public MyByteArrayOutputStream(int size) {
			super(size);
		}

		public int getCount() {
			return count;
		}

		public byte[] getBuf() {
			return buf;
		}
	}

	public int write(Object[] data, int offset, int length) throws IOException,
			InterruptedException, ExecutionException {

		final MyByteArrayOutputStream bAos;
		final ObjectOutputStream objOS;
		final AtomicBoolean canWrite;

		objOS = this.ooStream;
		bAos = this.bAos;
		canWrite = this.canWrite;

		while (!canWrite.get())
			Thread.sleep(10);

		int written = 0;
		while (written < length) {
			objOS.writeObject(data[offset++]);
			++written;
		}

		canWrite.set(false);
		ByteBuffer bb = ByteBuffer.wrap(bAos.getBuf(), 0, bAos.getCount());
		asyncSktChannel.write(bb, bb,
				new CompletionHandler<Integer, ByteBuffer>() {
					@Override
					public void completed(Integer result, ByteBuffer attachment) {

						if (attachment.hasRemaining()) {
							asyncSktChannel.write(attachment, attachment, this);
						} else {
							System.out.println("Completed.. ");
							canWrite.set(true);
							bAos.reset();
							try {
								objOS.reset();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

					@Override
					public void failed(Throwable exc, ByteBuffer attachment) {
						exc.printStackTrace();
						System.out.println("**************************");
						canWrite.set(false);
					}
				});

		return written;
	}
}
