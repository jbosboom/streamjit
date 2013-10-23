package edu.mit.streamjit.impl.distributed.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.OptionalDataException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.AbstractBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;

/**
 * This is {@link BoundaryInputChannel} over TCP. Receive objects from TCP
 * connection and write them into the given {@link Buffer}.
 * <p>
 * Note: TCPInputChannel acts as client when making TCP connection.
 * </p>
 * <p>
 * In some case, after Stop() is called, buffer might be full forever and there
 * might be more data in the kernel TCP buffer. In this case before extraBuffer
 * will be filled with all kernel data.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel implements BoundaryInputChannel {

	private final int debugPrint;

	private final Buffer buffer;

	private Buffer extraBuffer;

	private final TCPConnectionProvider conProvider;

	private final TCPConnectionInfo conInfo;

	private Connection tcpConnection;

	private final AtomicBoolean stopFlag;

	private final String name;

	private boolean softClosed;

	private boolean isClosed;

	int count;

	private ImmutableList<Object> unProcessedData;

	public TCPInputChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugPrint) {
		this.buffer = buffer;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
		this.stopFlag = new AtomicBoolean(false);
		this.name = "TCPInputChannel - " + bufferTokenName;
		this.debugPrint = debugPrint;
		this.softClosed = false;
		this.extraBuffer = null;
		this.unProcessedData = null;
		this.isClosed = false;
		count = 0;
	}

	@Override
	public void closeConnection() throws IOException {
		// tcpConnection.closeConnection();
		this.isClosed = true;
	}

	@Override
	public boolean isStillConnected() {
		return tcpConnection.isStillConnected();
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (tcpConnection == null || !tcpConnection.isStillConnected()) {
					try {
						tcpConnection = conProvider.getConnection(conInfo);
					} catch (IOException e) {
						// TODO: Need to handle this exception.
						e.printStackTrace();
					}
				}
				while (!stopFlag.get() && !softClosed) {
					receiveData();
				}

				if (!softClosed)
					finalReceive();

				try {
					closeConnection();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	@Override
	public void receiveData() {
		int bufFullCount = 0;
		try {
			Object obj = tcpConnection.readObject();
			count++;
			if (debugPrint == 3) {
				System.out.println(Thread.currentThread().getName() + " - "
						+ obj.toString());
			}

			while (!this.buffer.write(obj)) {
				if (debugPrint == 3) {
					System.out.println(Thread.currentThread().getName()
							+ " Buffer FULL - " + obj.toString());
				}
				try {
					// TODO: Need to tune the sleep time.
					Thread.sleep(5);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (stopFlag.get() && bufFullCount++ > 5) {
					this.extraBuffer = new ExtraBuffer();
					extraBuffer.write(obj);
					System.err
							.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					System.err.println(name
							+ " Writing extra data in to extra buffer");
					System.err
							.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					break;
				}
			}

			if (count % 1000 == 0 && debugPrint == 2) {
				System.out.println(Thread.currentThread().getName() + " - "
						+ count + " no of items have been received");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (OptionalDataException e) {
			softClosed = true;
		} catch (EOFException e) {
			// Other side is closed.
			stopFlag.set(true);
		} catch (IOException e) {
			// TODO: Verify the program quality. Try to reconnect until it
			// is told to stop.
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			reConnect();
		}
	}

	/**
	 * Once this channel is asked to stop, we have to read all data that exists
	 * in the kernel's TCP buffer. Otherwise those data will be lost forever.
	 */
	private void finalReceive() {
		boolean hasData;
		int bufFullCount;
		Buffer buffer;
		if (this.extraBuffer == null)
			buffer = this.buffer;
		else
			buffer = this.extraBuffer;
		do {
			bufFullCount = 0;
			try {
				Object obj = tcpConnection.readObject();
				count++;
				if (debugPrint == 3) {
					System.out.println(Thread.currentThread().getName()
							+ " finalReceive - " + obj.toString());
				}

				hasData = true;
				while (!buffer.write(obj)) {
					if (debugPrint == 3) {
						System.out.println(Thread.currentThread().getName()
								+ " finalReceive:Buffer FULL - "
								+ obj.toString());
					}
					try {
						Thread.sleep(20);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					if (bufFullCount++ > 5) {
						assert buffer != this.extraBuffer : "ExtraBuffer is full. This shouldn't be the case.";
						assert this.extraBuffer == null : "Extra buffer has already been created.";
						this.extraBuffer = new ExtraBuffer();
						extraBuffer.write(obj);
						buffer = extraBuffer;
						System.err
								.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
						System.err.println(name
								+ " Writing extra data in to extra buffer");
						System.err
								.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					}
				}

				if (count % 1000 == 0 && debugPrint == 2) {
					System.out.println(Thread.currentThread().getName() + " - "
							+ count + " no of items have been received");
				}

			} catch (ClassNotFoundException e) {
				hasData = true;
				e.printStackTrace();
			} catch (OptionalDataException e) {
				softClosed = true;
				hasData = false;
			} catch (IOException e) {
				hasData = false;
			}
		} while (hasData);
	}

	private void reConnect() {
		while (!stopFlag.get()) {
			try {
				System.out.println("TCPInputChannel : Reconnecting...");
				this.tcpConnection.closeConnection();
				tcpConnection = conProvider.getConnection(conInfo);
				return;
			} catch (IOException e) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	@Override
	public int getOtherNodeID() {
		return 0;
	}

	@Override
	public void stop() {
		this.stopFlag.set(true);
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public Buffer getExtraBuffer() {
		return extraBuffer;
	}

	/**
	 * Another buffer implementation. Not thread safe.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Oct 17, 2013
	 */
	private class ExtraBuffer extends AbstractBuffer {

		private final Queue<Object> queue;

		public ExtraBuffer() {
			this.queue = new ArrayDeque<>();
		}

		@Override
		public Object read() {
			return queue.poll();
		}

		@Override
		public boolean write(Object t) {
			return queue.offer(t);
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public int capacity() {
			return Integer.MAX_VALUE;
		}
	}

	// TODO: Huge data copying is happening in this code three times. Need to
	// optimise it.
	private void fillUnprocessedData() {
		System.out.println(name + " - Buffer size is - " + buffer.size());
		Object[] bufArray = new Object[buffer.size()];
		buffer.readAll(bufArray);
		assert buffer.size() == 0 : String.format(
				"buffer size is %d. But 0 is expected", buffer.size());
		if (extraBuffer == null)
			this.unProcessedData = ImmutableList.copyOf(bufArray);
		else {
			System.out.println(name + " - Extra data buffer size is - "
					+ extraBuffer.size());
			Object[] exArray = new Object[extraBuffer.size()];
			extraBuffer.readAll(exArray);
			assert extraBuffer.size() == 0 : String.format(
					"extraBuffer size is %d. But 0 is expected",
					extraBuffer.size());

			Object[] mergedArray = new Object[bufArray.length + exArray.length];
			System.arraycopy(bufArray, 0, mergedArray, 0, bufArray.length);
			System.arraycopy(exArray, 0, mergedArray, bufArray.length,
					exArray.length);

			this.unProcessedData = ImmutableList.copyOf(mergedArray);
		}
	}

	@Override
	public ImmutableList<Object> getUnprocessedData() {
		if (!this.isClosed)
			throw new IllegalAccessError(
					"Still processing... No unprocessed data");

		if (unProcessedData == null)
			fillUnprocessedData();

		return unProcessedData;
	}
}
