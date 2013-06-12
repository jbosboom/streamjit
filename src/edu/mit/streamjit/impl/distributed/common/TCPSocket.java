package edu.mit.streamjit.impl.distributed.common;

import java.io.*;
import java.net.*;

public class TCPSocket {

	private ObjectOutput ooStream = null;
	private ObjectInput oiStream = null;
	private Socket socket = null;
	private boolean isconnected = false;

	public TCPSocket(Socket socket) {
		try {
			this.socket = socket;
			ooStream = new ObjectOutputStream(this.socket.getOutputStream());
			oiStream = new ObjectInputStream(this.socket.getInputStream());
			System.out.println("ObjectOutputStream created");
			isconnected = true;
			System.out.println("ObjectInputStream created");
		} catch (IOException iex) {
			isconnected = false;
			System.err.println(iex.toString());
			System.exit(1);
		}
	}

	public void sendObject(Object obj) throws IOException {
		if (isStillconnected()) {
			if (ooStream != null) {
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
				throw new IOException("Connection not established");
			}
		} else {
			System.out.println("socket closed");
			System.exit(0);
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
			System.err.println(ex.toString());
			System.exit(0);
		}
	}

	public final boolean isStillconnected() {
		// return (this.socket.isConnected() && !this.socket.isClosed());
		return isconnected;
	}

	/**
	 * Unfortunately we need to pass the Class<T> klass as the argument here to successfully cast the object to required return type.
	 * The following code doesn't work.... <code>
	 	public <T> T receiveObject() {
	 		Object o;
	 		...
	  		return (T)o;
	 		}
	 </code>
	 */
	// public <T> T receiveObject(Class<T> Klass) throws IOException, ClassNotFoundException{
	// T cb = null;
	// Class<T> Klass1 = (Class<T>) cb.getClass();
	// if (isStillconnected()) {
	// Object o = null;
	//
	// try {
	// o = oiStream.readObject();
	// // System.out.println("DEBUG: tostring = " + o.toString());
	// // System.out.println("DEBUG: getClass = " + o.getClass());
	// cb = (T)o;
	// } catch (ClassCastException e) {
	// // If unknown object then ignore it.
	// System.out.println(o.toString());
	// } catch (ClassNotFoundException ex) {
	// // If unknown object then ignore it.
	// // System.out.println(o.toString());
	// throw ex;
	// } catch (IOException e) {
	// //e.printStackTrace();
	// isconnected = false;
	// throw e;
	// }
	// } else {
	// System.out.println("socket closed");
	// System.exit(0);
	// }
	// return cb; // TODO Need to handle this.
	// }

	public <T> T receiveObject() throws IOException, ClassNotFoundException {
		T cb = null;
		if (isStillconnected()) {
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
			System.out.println("socket closed");
			System.exit(0);
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