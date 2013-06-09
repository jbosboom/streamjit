package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryInputChannel;
import edu.mit.streamjit.impl.interp.Channel;

/**
 * TCPInputChannel acts as client when making TCP connection.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPInputChannel<E> implements BoundaryInputChannel<E> {

	Channel<E> channel;

	SlaveConnection inputConnection;

	private volatile boolean stopFlag;

	TCPInputChannel(Channel<E> channel, String IpAddress, int portNo) {
		this.channel = channel;
		this.stopFlag = false;
		this.inputConnection = new SlaveTCPConnection(IpAddress, portNo);
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
	public void makeConnection() throws IOException {

		inputConnection.makeConnection();
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (inputConnection == null || !inputConnection.isStillConnected()) {
					try {
						makeConnection();
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
