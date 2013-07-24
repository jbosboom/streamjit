package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.api.NodeInfo;
import edu.mit.streamjit.impl.distributed.node.Connection;

/**
 * {@link CommunicationManager} manages all type of communications. keep track
 * of all {@link Connection} and manages them. TODO: Need to handle the
 * connection loss and reconnection. Assigns machineID based on the connecting
 * order.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
public interface CommunicationManager {

	/**
	 * The type of communication between two machine nodes.
	 */
	public enum CommunicationType {
		TCP, LOCAL, UDP, BUFFER;
	}

	public <T> T readObject(int machineID) throws IOException,
			ClassNotFoundException;

	public void writeObject(int machineID, Object obj) throws IOException;

	// blocking call
	/**
	 * Makes all connections based on the argument comTypeCount. But only one
	 * local connection can be created. So the value for the key
	 * {@link CommunicationType}.LOCAL in the argument comTypeCount has no
	 * effect.
	 * 
	 * @param comTypeCount
	 *            Map that tells the number of connections need to be
	 *            established for each {@link CommunicationType}.
	 * @throws IOException
	 */
	public void connectMachines(Map<CommunicationType, Integer> comTypeCount)
			throws IOException;

	// non blocking call
	public void connectMachines(long timeOut) throws IOException;

	public void closeAllConnections() throws IOException;

	public void closeConnection(int machineID) throws IOException;

	public boolean isConnected(int machineID);

	/**
	 * MachineID 0 is reserved for controller node.
	 */
	public List<Integer> getConnectedMachineIDs();

}