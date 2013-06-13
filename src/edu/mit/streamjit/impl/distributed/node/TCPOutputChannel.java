/**
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.api.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.TCPConnection;
import edu.mit.streamjit.impl.distributed.runtimer.ListenerSocket;
import edu.mit.streamjit.impl.interp.Channel;

public class TCPOutputChannel<E> implements BoundaryOutputChannel<E> {

	int portNo;

	private volatile boolean stopFlag;

	private Connection tcpConnection;
	
	private Channel<E> channel;

	public TCPOutputChannel(Channel<E> channel, int portNo) {
		this.channel = channel;
		this.portNo = portNo;
		this.stopFlag = false;
	}

	@Override
	public void closeConnection() throws IOException {
		tcpConnection.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		return (tcpConnection == null) ? false : tcpConnection.isStillConnected();
	}

	private void makeConnection() throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(this.portNo, 1);
		// As we need only one connection, lets run the accepting process in this caller thread rather that spawning a new thread.
		listnerSckt.run();
		this.tcpConnection = listnerSckt.getAcceptedSockets().get(0);
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (tcpConnection == null || !tcpConnection.isStillConnected()) {
					try {
						makeConnection();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				while (!stopFlag)
					sendData();

				try {
					closeConnection();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public void sendData() {
		while (!this.channel.isEmpty()) {
			try {
				tcpConnection.writeObject(channel.pop());
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