package edu.mit.streamjit.impl.distributed.node;

import java.io.EOFException;
import java.io.IOException;
import java.io.OptionalDataException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel implements BoundaryInputChannel {

	private final int debugPrint;

	private Buffer buffer;

	private TCPConnectionProvider conProvider;

	private TCPConnectionInfo conInfo;

	private Connection tcpConnection;

	private AtomicBoolean stopFlag;

	private String name;

	private boolean softClosed;

	int count;

	public TCPInputChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugPrint) {
		this.buffer = buffer;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
		this.stopFlag = new AtomicBoolean(false);
		this.name = "TCPInputChannel - " + bufferTokenName;
		this.debugPrint = debugPrint;
		this.softClosed = false;
		count = 0;
	}

	@Override
	public void closeConnection() throws IOException {
		// tcpConnection.closeConnection();
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
		try {
			Object obj = tcpConnection.readObject();
			count++;
			if (debugPrint == 3) {
				System.out.println(Thread.currentThread().getName() + " - "
						+ obj.toString());
			}

			while (!this.buffer.write(obj)) {
				try {
					// TODO: Need to tune the sleep time.
					// System.out.println("InputChannel : Buffer full");
					if (debugPrint == 3) {
						System.out.println(Thread.currentThread().getName()
								+ " Buffer FULL - " + obj.toString());
					}
					Thread.sleep(5);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
		do {
			try {
				Object obj = tcpConnection.readObject();
				count++;
				if (debugPrint == 3) {
					System.out.println(Thread.currentThread().getName()
							+ " finalReceive - " + obj.toString());
				}

				hasData = true;
				while (!this.buffer.write(obj)) {
					try {
						// TODO: Need to tune the sleep time.
						// TODO : Need to handle the situation if the buffer
						// becomes full forever. ( Other worker thread is
						// stopped and not consuming any data.)
						// System.out.println("InputChannel : Buffer full");
						if (debugPrint == 3) {
							System.out.println(Thread.currentThread().getName()
									+ " finalReceive:Buffer FULL - "
									+ obj.toString());
						}
						Thread.sleep(5);
					} catch (InterruptedException e) {
						e.printStackTrace();
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
}
