package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.TCPConnection;
import edu.mit.streamjit.impl.distributed.runtimer.ListenerSocket;

/**
 * This is {@link BoundaryOutputChannel} over TCP. Reads data from the given
 * {@link Buffer} and send them over the TCP connection.
 * <p>
 * Note: TCPOutputChannel acts as server when making TCP connection.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public class TCPOutputChannel implements BoundaryOutputChannel {

	private int portNo;

	private AtomicBoolean stopFlag;

	private Connection tcpConnection;

	private Buffer buffer;

	public TCPOutputChannel(Buffer buffer, int portNo) {
		this.buffer = buffer;
		this.portNo = portNo;
		this.stopFlag = new AtomicBoolean(false);
	}

	@Override
	public void closeConnection() throws IOException {
		tcpConnection.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		return (tcpConnection == null) ? false : tcpConnection
				.isStillConnected();
	}

	private void makeConnection() throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(this.portNo, 1);
		// As we need only one connection, lets run the accepting process in
		// this caller thread rather that spawning a new thread.
		listnerSckt.run();
		Socket socket = listnerSckt.getAcceptedSockets().get(0);
		this.tcpConnection = new TCPConnection(socket);
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
						// TODO: Need to handle this exception.
						e.printStackTrace();
					}
				}
				while (!stopFlag.get())
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
		while (this.buffer.size() > 0 && !stopFlag.get()) {
			try {
				tcpConnection.writeObject(buffer.read());
			} catch (IOException e) {
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
		ListenerSocket lstnSckt;
		try {
			lstnSckt = new ListenerSocket(portNo);
			this.tcpConnection.closeConnection();
			while (!stopFlag.get()) {
				System.out.println("TCPOutputChannel : Reconnecting...");
				try {
					Socket skt = lstnSckt.makeConnection(1000);
					this.tcpConnection = new TCPConnection(skt);
					return;
				} catch (SocketTimeoutException stex) {
					// We make this exception to recheck the stopFlag. Otherwise
					// thread will get struck at server.accept().
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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
