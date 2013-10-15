package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.io.Serializable;

import com.google.common.collect.ComparisonChain;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * Communication interface for both {@link StreamNode} and {@link Controller}
 * side. This interface is for an IO connection that is already created, i.e.,
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
	 * Checks whether the connection is still open or not.
	 * 
	 * @return true if the connection is open and valid.
	 */
	public boolean isStillConnected();

	/**
	 * Describes a connection between two machines. ConnectionInfo is considered
	 * symmetric for equal() and hashCode() calculation. As long as same
	 * machineIDs are involved, irrespect of srcID and dstID positions, these
	 * methods return same result.
	 */
	public class ConnectionInfo implements Serializable {

		private int srcID;

		private int dstID;

		public ConnectionInfo(int srcID, int dstID) {
			this.srcID = srcID;
			this.dstID = dstID;
		}

		public int getSrcID() {
			return srcID;
		}

		public int getDstID() {
			return dstID;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			int min = Math.min(srcID, dstID);
			int max = Math.max(srcID, dstID);
			result = prime * result + min;
			result = prime * result + max;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ConnectionInfo other = (ConnectionInfo) obj;
			int myMin = Math.min(srcID, dstID);
			int myMax = Math.max(srcID, dstID);
			int otherMin = Math.min(other.srcID, other.dstID);
			int otherMax = Math.max(other.srcID, other.dstID);
			if (myMin != otherMin)
				return false;
			if (myMax != otherMax)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "ConnectionInfo [srcID=" + srcID + ", dstID=" + dstID + "]";
		}
	}
}
