/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.net.InetAddress;
import java.net.Socket;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.node.BlockingInputChannel;
import edu.mit.streamjit.impl.distributed.node.BlockingOutputChannel;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * TCPConnection is not thread safe.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 16, 2013
 * 
 */
public class TCPConnection implements Connection {

	private ObjectOutputStream ooStream = null;
	private ObjectInput oiStream = null;
	private Socket socket = null;
	private boolean isconnected = false;
	private final int resetCount;

	// For debugging purpose: Just to count the number of TCP connections made.
	private static int count = 0;

	public TCPConnection(Socket socket) {
		this(socket, 5000);
	}

	/**
	 * @param socket
	 * @param resetCount
	 *            reset the {@link ObjectOutputStream} after this no of sends.
	 *            To avoid out of memory error.
	 */
	public TCPConnection(Socket socket, int resetCount) {
		this.resetCount = resetCount;
		try {
			this.socket = socket;
			ooStream = new ObjectOutputStream(this.socket.getOutputStream());
			oiStream = new ObjectInputStream(this.socket.getInputStream());
			isconnected = true;
			// System.out.println(String.format(
			// "DEBUG: TCP connection %d has been established", count++));
		} catch (IOException iex) {
			isconnected = false;
			iex.printStackTrace();
		}
	}

	// This is introduced to reduce the ooStream.reset(); frequency. Too many
	// resets, i.e., reset the ooStream for every new write severely affects the
	// performance.
	int n = 0;

	@Override
	public void writeObject(Object obj) throws IOException {
		if (isStillConnected()) {
			try {
				ooStream.writeObject(obj);

				n++;
				// TODO: Any way to improve the performance?
				if (n > resetCount) {
					n = 0;
					ooStream.reset();
				}
				// System.out.println("Object send...");
			} catch (IOException ix) {
				// Following doesn't change when other side of the socket is
				// closed.....
				/*
				 * System.out.println("socket.isBound()" + socket.isBound());
				 * System.out.println("socket.isClosed()" + socket.isClosed());
				 * System.out.println("socket.isConnected()" +
				 * socket.isConnected());
				 * System.out.println("socket.isInputShutdown()" +
				 * socket.isInputShutdown());
				 * System.out.println("socket.isOutputShutdown()" +
				 * socket.isOutputShutdown());
				 */
				isconnected = false;
				throw ix;
			}
		} else {
			throw new IOException("TCPConnection: Socket is not connected");
		}
	}

	public final void closeConnection() {
		isconnected = false;
		try {
			if (ooStream != null)
				this.ooStream.close();
			if (oiStream != null)
				this.oiStream.close();
			if (socket != null)
				this.socket.close();
		} catch (IOException ex) {
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
			} catch (OptionalDataException ex) {
				// System.err.println("OptionalDataException....SoftClose...");
				int a = oiStream.read();
				// System.out.println(a);
				throw ex;
			} catch (ClassCastException e) {
				System.err.println("ClassCastException...");
				// If unknown object then ignore it.
				System.out.println(o.toString());
			} catch (ClassNotFoundException ex) {
				// If unknown object then ignore it.
				// System.out.println(o.toString());
				System.err.println("ClassNotFoundException...");
				throw ex;
			} catch (IOException e) {
				// e.printStackTrace();
				System.err.println("IOException...");
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

	@Override
	public void softClose() throws IOException {
		this.ooStream.write('\u001a');
		this.ooStream.flush();
	}

	/**
	 * Uniquely identifies a TCP connection among all connected machines.
	 * 
	 * <p>
	 * NOTE: IPAddress is not included for the moment to avoid re-sending same
	 * information again and again for every reconfiguration. machineId to
	 * {@link NodeInfo} map will be sent initially. So {@link StreamNode}s can
	 * get ipAddress of a machine from that map.
	 */
	public static class TCPConnectionInfo extends ConnectionInfo {

		private static final long serialVersionUID = 1L;

		private final int portNo;

		public TCPConnectionInfo(int srcID, int dstID, int portNo) {
			super(srcID, dstID, true);
			Ipv4Validator validator = Ipv4Validator.getInstance();
			if (!validator.isValid(portNo))
				throw new IllegalArgumentException("Invalid port No");
			this.portNo = portNo;
		}

		public int getPortNo() {
			return portNo;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + portNo;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (getClass() != obj.getClass())
				return false;
			TCPConnectionInfo other = (TCPConnectionInfo) obj;
			if (portNo != other.portNo)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "TCPConnectionInfo [srcID=" + getSrcID() + ", dstID="
					+ getDstID() + ", portID=" + portNo + "]";
		}

		@Override
		public Connection makeConnection(int nodeID, NetworkInfo networkInfo,
				int timeOut) {
			Connection con = null;
			if (srcID == nodeID) {
				try {
					con = ConnectionFactory.getConnection(portNo, timeOut,
							false);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			else if (dstID == nodeID) {
				InetAddress ipAddress = networkInfo.getInetAddress(srcID);
				try {
					con = ConnectionFactory.getConnection(
							ipAddress.getHostAddress(), portNo, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				throw new IllegalArgumentException(
						"Neither srcID nor dstID matches with nodeID");
			}
			return con;
		}

		@Override
		public BoundaryInputChannel inputChannel(Token t, int bufSize,
				ConnectionProvider conProvider) {
			return new BlockingInputChannel(bufSize, conProvider, this,
					t.toString(), 0);
		}

		@Override
		public BoundaryOutputChannel outputChannel(Token t, int bufSize,
				ConnectionProvider conProvider) {
			return new BlockingOutputChannel(bufSize, conProvider, this,
					t.toString(), 0);
		}
	}
}