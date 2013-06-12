package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.api.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * TCPInputChannel acts as client when making TCP connection.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel<E> implements BoundaryInputChannel<E> {

	private Channel<E> channel;

	private String ipAddress;

	private int portNo;

	private Connection inputConnection;

	private volatile boolean stopFlag;

	public TCPInputChannel(Channel<E> channel, String ipAddress, int portNo) {
		this.channel = channel;
		this.ipAddress = ipAddress;
		this.portNo = portNo;
		this.stopFlag = false;
	}

	@Override
	public void closeConnection() throws IOException {
		inputConnection.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		return inputConnection.isStillConnected();
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (inputConnection == null || !inputConnection.isStillConnected()) {
					try {
						ConnectionFactory cf = new ConnectionFactory();
						cf.getConnection(ipAddress, portNo);
					} catch (IOException e) {
						stopFlag = true;
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
		while (!stopFlag) {
			try {
				E element = inputConnection.readObject();
				// TODO: need to confirm the channel have enough capacity to accept elements.
				// Consider adding channel.getMaxSize() function. Already Channel.getSize is available.
				this.channel.push(element);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int getOtherMachineID() {
		return 0;
	}

	@Override
	public void stop() {
		this.stopFlag = true;
	}
}
