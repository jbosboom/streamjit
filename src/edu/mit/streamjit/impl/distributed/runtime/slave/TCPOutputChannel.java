/**
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.IOException;
import java.util.List;

import edu.mit.streamjit.impl.distributed.runtime.api.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.runtime.common.TCPSocket;
import edu.mit.streamjit.impl.distributed.runtime.master.ListenerSocket;
import edu.mit.streamjit.impl.interp.Channel;

public class TCPOutputChannel<E> implements BoundaryOutputChannel<E> {

	int portNo;

	TCPSocket socket;
	Channel<E> channel;

	public TCPOutputChannel(Channel<E> channel, int portNo) {
		this.channel = channel;
		this.portNo = portNo;
	}

	@Override
	public void closeConnection() throws IOException {
		socket.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		return socket.isStillconnected();
	}

	@Override
	public void makeConnection() throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(this.portNo, 1);
		// As we need only one connection, lets run the accepting process in this caller thread rather that spawning a new thread.
		listnerSckt.run();
		this.socket = listnerSckt.getAcceptedSockets().get(0);
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				sendData();
			}
		};
	}

	private void sendData() {
		while (true) {
			if (!this.channel.isEmpty()) {
				try {
					socket.sendObject(channel.pop());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public int getOtherMachineID() {
		return 0;
	}
}
