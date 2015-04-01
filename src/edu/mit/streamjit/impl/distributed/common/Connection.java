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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * Communication interface for an IO connection that is already created, i.e.,
 * creating a connections is not handled at here. Consider
 * {@link ConnectionFactory} to create a connection. </p> For the moment,
 * communicates at object granularity level. We may need to add primitive
 * interface functions later.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public interface Connection {

	/**
	 * Read an object from this connection.
	 * 
	 * @return Received object.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 *             If the object received is not the type of T.
	 */
	public <T> T readObject() throws IOException, ClassNotFoundException;

	/**
	 * Write a object to the connection. </p>throws exception if failed. So no
	 * return value needed.
	 * 
	 * @throws IOException
	 */
	public void writeObject(Object obj) throws IOException;

	/**
	 * Close the connection. This function is responsible for all kind of
	 * resource cleanup. </p>throws exception if failed. So no return value
	 * needed.
	 * 
	 * @throws IOException
	 */
	public void closeConnection() throws IOException;

	/**
	 * Do not close the underlying real connection. Instead inform other side to
	 * the current communication session is over.
	 * <p>
	 * This is introduced because {@link ObjectInputStream#readObject()} eats
	 * thread InterruptedException and yields no way to close the reader thread
	 * when the thread is blocked at {@link ObjectInputStream#readObject()}
	 * method call.
	 * </p>
	 * 
	 * @throws IOException
	 */
	public void softClose() throws IOException;

	/**
	 * Checks whether the connection is still open or not.
	 * 
	 * @return true if the connection is open and valid.
	 */
	public boolean isStillConnected();

	/**
	 * Describes a connection between two machines.
	 * <ol>
	 * <li>if isSymmetric is <code>true</code>, ConnectionInfo is considered
	 * symmetric for equal() and hashCode() calculation. As long as same
	 * machineIDs are involved, irrespect of srcID and dstID positions, these
	 * methods return same result.
	 * <li>
	 * if isSymmetric is <code>false</code> srcID and dstID will be treated as
	 * not interchangeable entities.
	 * </ol>
	 * 
	 * <p>
	 * <b>Note : </b> All instances of ConnectionInfo, including subclass
	 * instances, will be equal to each other if the IDs matches. See the
	 * hashCode() and equals() methods. <b>The whole point of this class is to
	 * identify a connection between two machines.</b>
	 */
	public abstract class ConnectionInfo implements Serializable {

		private static final long serialVersionUID = 1L;

		protected final int srcID;

		protected final int dstID;

		/**
		 * Tells whether this connection is symmetric or not.
		 */
		protected final boolean isSymmetric;

		public ConnectionInfo(int srcID, int dstID) {
			this(srcID, dstID, true);
		}

		protected ConnectionInfo(int srcID, int dstID, boolean isSymmetric) {
			this.srcID = srcID;
			this.dstID = dstID;
			this.isSymmetric = isSymmetric;
		}

		public int getSrcID() {
			return srcID;
		}

		public int getDstID() {
			return dstID;
		}

		public boolean isSymmetric() {
			return isSymmetric;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			if (isSymmetric) {
				int min = Math.min(srcID, dstID);
				int max = Math.max(srcID, dstID);
				result = prime * result + min;
				result = prime * result + max;
			} else {
				result = prime * result + srcID;
				result = prime * result + dstID;
			}
			result = prime * result + (isSymmetric ? 1231 : 1237);
			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object) equals() overwritten
		 * here breaks the reflexive(), symmetric() and transitive() properties,
		 * especially when subclasses involves. The purpose of this overwriting
		 * is to check whether an already established connection could be
		 * reused.
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof ConnectionInfo))
				return false;
			ConnectionInfo other = (ConnectionInfo) obj;
			if (other.isSymmetric) {
				int myMin = Math.min(srcID, dstID);
				int myMax = Math.max(srcID, dstID);
				int otherMin = Math.min(other.srcID, other.dstID);
				int otherMax = Math.max(other.srcID, other.dstID);
				if (myMin != otherMin)
					return false;
				if (myMax != otherMax)
					return false;
			} else {
				if (srcID != other.srcID)
					return false;
				if (dstID != other.dstID)
					return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return String.format(
					"ConnectionInfo [srcID=%d, dstID=%d, isSymmetric=%s]",
					srcID, dstID, isSymmetric);
		}

		/**
		 * This function will establish a new connection according to the
		 * connection info.
		 * 
		 * @param nodeID
		 *            : nodeID of the {@link StreamNode} that invokes this
		 *            method.
		 * @param networkInfo
		 *            : network info of the system.
		 * @return {@link Connection} that is described by this
		 *         {@link ConnectionInfo}.
		 */
		public abstract Connection makeConnection(int nodeID,
				NetworkInfo networkInfo, int timeOut);

		public abstract BoundaryInputChannel inputChannel(Token t, int bufSize,
				ConnectionProvider conProvider);

		public abstract BoundaryOutputChannel outputChannel(Token t,
				int bufSize, ConnectionProvider conProvider);
	}

	/**
	 * We need an instance of {@link ConnectionInfo} to compare and get a
	 * concrete {@link ConnectionInfo} from the list of already created
	 * {@link ConnectionInfo}s. This class is added for that purpose.
	 */
	public static class GenericConnectionInfo extends ConnectionInfo {

		private static final long serialVersionUID = 1L;

		public GenericConnectionInfo(int srcID, int dstID) {
			super(srcID, dstID);
		}

		public GenericConnectionInfo(int srcID, int dstID, boolean isSymmetric) {
			super(srcID, dstID, isSymmetric);
		}

		@Override
		public Connection makeConnection(int nodeID, NetworkInfo networkInfo,
				int timeOut) {
			throw new java.lang.Error("This method is not supposed to call");
		}

		@Override
		public BoundaryInputChannel inputChannel(Token t, int bufSize,
				ConnectionProvider conProvider) {
			throw new java.lang.Error("This method is not supposed to call");
		}

		@Override
		public BoundaryOutputChannel outputChannel(Token t, int bufSize,
				ConnectionProvider conProvider) {
			throw new java.lang.Error("This method is not supposed to call");
		}
	}

	/**
	 * ConnectionType serves two purposes
	 * <ol>
	 * <li>Tune the connections. This will passed to opentuner.
	 * <li>Indicate the {@link StreamNode} to create appropriate
	 * {@link BoundaryChannel}. This will be bound with {@link ConnectionInfo}.
	 * </ol>
	 */
	public enum ConnectionType {
		/**
		 * Blocking TCP socket connection
		 */
		BTCP, /**
		 * Non-Blocking TCP socket connection
		 * 
		 * NBTCP,
		 */
		/**
		 * Asynchronous TCP socket connection
		 */
		ATCP,
		/**
		 * Blocking InfiniBand
		 * 
		 * BIB,
		 */
		/**
		 * Non-Blocking InfiniBand
		 * 
		 * NBIB
		 */
	}

	/**
	 * Keeps all opened {@link TCPConnection}s for a machine. Each machine
	 * should have a single instance of this class and use this class to make
	 * new connections.
	 * 
	 * <p>
	 * TODO: Need to make this class singleton. I didn't do it now because in
	 * current way, controller and a local {@link StreamNode} are running in a
	 * same JVM. So first, local {@link StreamNode} should be made to run on a
	 * different JVM and then make this class singleton.
	 */
	public static class ConnectionProvider {

		private ConcurrentMap<ConnectionInfo, Connection> allConnections;

		private final int myNodeID;

		private final NetworkInfo networkInfo;

		public ConnectionProvider(int myNodeID, NetworkInfo networkInfo) {
			checkNotNull(networkInfo, "networkInfo is null");
			this.myNodeID = myNodeID;
			this.networkInfo = networkInfo;
			this.allConnections = new ConcurrentHashMap<>();
		}

		/**
		 * See {@link #getConnection(TCPConnectionInfo, int)}.
		 * 
		 * @param conInfo
		 * @return
		 * @throws IOException
		 */
		public Connection getConnection(ConnectionInfo conInfo)
				throws IOException {
			return getConnection(conInfo, 0);
		}

/**
		 * If the connection corresponds to conInfo is already established
		 * returns the connection. Try to make a new connection otherwise.
		 *
		 * @param conInfo - Information that uniquely identifies a {@link TCPConnection
		 * @param timeOut - Time out only valid if making connection needs to be
		 * 			done through a listener socket. i.e, conInfo.getSrcID() == myNodeID.
		 * @return
		 * @throws SocketTimeoutException
		 * @throws IOException
		 */
		public Connection getConnection(ConnectionInfo conInfo, int timeOut)
				throws SocketTimeoutException, IOException {
			Connection con = allConnections.get(conInfo);
			if (con != null) {
				if (con.isStillConnected()) {
					return con;
				} else {
					throw new AssertionError("con.closeConnection()");
					// con.closeConnection();
				}
			}

			con = conInfo.makeConnection(myNodeID, networkInfo, timeOut);
			if (con == null)
				throw new IOException("Connection making process failed.");

			allConnections.put(conInfo, con);
			return con;
		}

		public void closeAllConnections() {
			for (Connection con : allConnections.values()) {
				try {
					con.closeConnection();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
