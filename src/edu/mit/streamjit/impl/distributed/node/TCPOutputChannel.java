package edu.mit.streamjit.impl.distributed.node;

import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;

/**
 * This is {@link BoundaryOutputChannel} over TCP. Reads data from the given
 * {@link Buffer} and send them over the TCP connection.
 * <p>
 * Note: TCPOutputChannel acts as server when making TCP connection.
 * </p>
 * <p>
 * TODO: Need to aggressively optimise this class.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPOutputChannel implements BoundaryOutputChannel {

	private final FileWriter writer;

	private final int debugLevel;

	private final Buffer buffer;

	private final TCPConnectionProvider conProvider;

	private final ConnectionInfo conInfo;

	private Connection tcpConnection;

	private final AtomicBoolean stopFlag;

	private final String name;

	private volatile boolean isFinal;

	private int count;

	protected ImmutableList<Object> unProcessedData;

	public TCPOutputChannel(int bufSize, TCPConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
		this(new ConcurrentArrayBuffer(bufSize), conProvider, conInfo,
				bufferTokenName, debugLevel);
	}

	public TCPOutputChannel(Buffer buffer, TCPConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName, int debugLevel) {
		this.buffer = buffer;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
		this.stopFlag = new AtomicBoolean(false);
		this.isFinal = false;
		this.name = "TCPOutputChannel - " + bufferTokenName;
		this.debugLevel = debugLevel;
		this.unProcessedData = null;
		count = 0;

		FileWriter w = null;
		if (this.debugLevel == 5) {
			try {
				w = new FileWriter(name, true);
				w.write("---------------------------------\n");
			} catch (IOException e) {
				w = null;
				e.printStackTrace();
			}
		}
		writer = w;
	}

	private void closeConnection() throws IOException {
		// tcpConnection.closeConnection();
		tcpConnection.softClose();
	}

	@Override
	public final Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (tcpConnection == null || !tcpConnection.isStillConnected()) {
					try {
						tcpConnection = conProvider.getConnection(conInfo);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				while (!stopFlag.get())
					sendData();

				if (isFinal)
					finalSend();

				try {
					closeConnection();
				} catch (IOException e) {
					e.printStackTrace();
				}

				fillUnprocessedData();
				if (writer != null) {
					try {
						writer.flush();
						writer.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (debugLevel > 0) {
					System.err.println(Thread.currentThread().getName()
							+ " - Exiting...");
					System.out.println("isFinal " + isFinal);
					System.out.println("stopFlag " + stopFlag.get());
				}
			}
		};
	}

	public final void sendData() {
		while (this.buffer.size() > 0 && !stopFlag.get()) {
			try {
				Object obj = buffer.read();
				tcpConnection.writeObject(obj);
				count++;

				if (debugLevel == 3) {
					System.out.println(Thread.currentThread().getName() + " - "
							+ obj.toString());
				}

				if (writer != null) {
					writer.write(obj.toString());
					writer.write('\n');
				}
			} catch (IOException e) {
				System.err
						.println("TCP Output Channel. WriteObject exception.");
				reConnect();
			}
			if (count % 1000 == 0 && debugLevel == 2) {
				System.out.println(Thread.currentThread().getName() + " - "
						+ count + " items have been sent");
			}
		}
	}

	@Override
	public final void stop(boolean isFinal) {
		if (debugLevel > 0)
			System.out.println(Thread.currentThread().getName()
					+ " - stop request");
		if (!this.stopFlag.get()) {
			this.isFinal = isFinal;
			this.stopFlag.set(true);
		} else if (debugLevel > 0) {
			System.err.println("Stop has already been called.");
		}
	}

	/**
	 * This can be called when running the application with the final scheduling
	 * configurations. Shouldn't be called when autotuner tunes.
	 */
	private void finalSend() {
		while (this.buffer.size() > 0) {
			try {
				Object o = buffer.read();
				tcpConnection.writeObject(o);
				count++;

				if (debugLevel == 3) {
					System.out.println(Thread.currentThread().getName()
							+ " FinalSend - " + o.toString());
				}

				if (writer != null) {
					writer.write(o.toString());
					writer.write('\n');
				}

			} catch (IOException e) {
				System.err.println("TCP Output Channel. finalSend exception.");
			}
			if (count % 1000 == 0 && debugLevel == 2) {
				System.out.println(Thread.currentThread().getName()
						+ " FinalSend - " + count
						+ " no of items have been sent");
			}
		}
	}

	private void reConnect() {
		try {
			this.tcpConnection.closeConnection();
			while (!stopFlag.get()) {
				System.out.println("TCPOutputChannel : Reconnecting...");
				try {
					this.tcpConnection = conProvider.getConnection(conInfo,
							1000);
					return;
				} catch (SocketTimeoutException stex) {
					// We make this exception to recheck the stopFlag. Otherwise
					// thread will get struck at server.accept().
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public final String name() {
		return name;
	}

	// TODO: Huge data copying is happening in this code twice. Need to optimise
	// this.
	protected void fillUnprocessedData() {
		Object[] obArray = new Object[buffer.size()];
		buffer.readAll(obArray);
		assert buffer.size() == 0 : String.format(
				"buffer size is %d. But 0 is expected", buffer.size());
		this.unProcessedData = ImmutableList.copyOf(obArray);
	}

	@Override
	public ImmutableList<Object> getUnprocessedData() {
		if (unProcessedData == null)
			throw new IllegalAccessError(
					"Still processing... No unprocessed data");
		return unProcessedData;
	}

	@Override
	public Connection getConnection() {
		return tcpConnection;
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return conInfo;
	}

	@Override
	public Buffer getBuffer() {
		return buffer;
	}
}
