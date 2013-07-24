package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * TCPInputChannel acts as client when making TCP connection.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel<E> implements BoundaryInputChannel {

	private Channel<E> channel;

	private String ipAddress;

	private int portNo;

	private Connection tcpConnection;

	private AtomicBoolean stopFlag;

	public TCPInputChannel(Channel<E> channel, String ipAddress, int portNo) {
		this.channel = channel;
		this.ipAddress = ipAddress;
		this.portNo = portNo;
		this.stopFlag = new AtomicBoolean(false);
	}

	@Override
	public void closeConnection() throws IOException {
		tcpConnection.closeConnection();
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
						ConnectionFactory cf = new ConnectionFactory();
						tcpConnection = cf.getConnection(ipAddress, portNo);
					} catch (IOException e) {
						// TODO: Need to handle this exception.
						e.printStackTrace();
					}
				}
				receiveData();
				try {
					closeConnection();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
	}

	public void receiveData() {
		while (!stopFlag.get()) {
			try {
				E element = tcpConnection.readObject();
				// TODO: need to confirm the channel have enough capacity to
				// accept elements.
				// Consider adding channel.getMaxSize() function. Already
				// Channel.getSize is available.
				this.channel.push(element);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO: Verify the program quality. Try to reconnect until it
				// is told to stop.
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				reConnect();
			}
		}
	}

	private void reConnect() {
		while (!stopFlag.get()) {
			try {
				System.out.println("TCPInputChannel : Reconnecting...");
				this.tcpConnection.closeConnection();
				ConnectionFactory cf = new ConnectionFactory();
				tcpConnection = cf.getConnection(ipAddress, portNo);
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
}
