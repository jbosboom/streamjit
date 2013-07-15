package edu.mit.streamjit.impl.distributed.common;

import java.io.*;
import java.net.*;

import edu.mit.streamjit.impl.distributed.node.Connection;

public class TCPConnection implements Connection {

	private ObjectOutput ooStream = null;
	private ObjectInput oiStream = null;
	private Socket socket = null;
	private boolean isconnected = false;

	// For debugging purpose: Just to count the number of TCP connections made.
	private static int count = 0;

	public TCPConnection(Socket socket) {
		try {
			this.socket = socket;
			ooStream = new ObjectOutputStream(this.socket.getOutputStream());
			oiStream = new ObjectInputStream(this.socket.getInputStream());
			isconnected = true;
			System.out.println(String.format("DEBUG: TCP connection %d has been established", count++));
		} catch (IOException iex) {
			isconnected = false;
			iex.printStackTrace();
		}
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		if (isStillConnected()) {
			try {
				ooStream.writeObject(obj);
				// System.out.println("Object send...");
			} catch (IOException ix) {
				// Following doesn't change when other side of the socket is closed.....
				/*
				 * System.out.println("socket.isBound()" + socket.isBound()); System.out.println("socket.isClosed()" +
				 * socket.isClosed()); System.out.println("socket.isConnected()" + socket.isConnected());
				 * System.out.println("socket.isInputShutdown()" + socket.isInputShutdown());
				 * System.out.println("socket.isOutputShutdown()" + socket.isOutputShutdown());
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
			if (oiStream != null)
				this.oiStream.close();
			if (socket != null)
				this.socket.close();
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
		T cb = null;
		if (isStillConnected()) {
			Object o = null;
			try {
				o = oiStream.readObject();
				// System.out.println("DEBUG: tostring = " + o.toString());
				// System.out.println("DEBUG: getClass = " + o.getClass());
				// System.out.println("Object read...");
				cb = (T) o;
			} catch (ClassCastException e) {
				// If unknown object then ignore it.
				System.out.println(o.toString());
			} catch (ClassNotFoundException ex) {
				// If unknown object then ignore it.
				// System.out.println(o.toString());
				throw ex;
			} catch (IOException e) {
				// e.printStackTrace();
				isconnected = false;
				throw e;
			}
		} else {
			throw new IOException("TCPConnection: Socket is not connected");
		}
		return cb; // TODO Need to handle this.
	}

	public InetAddress getInetAddress() {
		if (socket != null)
			return this.socket.getInetAddress();
		else
			throw new NullPointerException("Socket is not initilized.");
	}
}