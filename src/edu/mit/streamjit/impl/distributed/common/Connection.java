package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

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
	public class ConnectionInfo implements Serializable {

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
		public Connection makeConnection(int nodeID, NetworkInfo networkInfo,
				int timeOut) {
			throw new java.lang.Error("This method is not supposed to call");
		}
	}
}
