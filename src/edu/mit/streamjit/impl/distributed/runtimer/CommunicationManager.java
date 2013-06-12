/**
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;

/**
 * assigns machine id and keeps the set of connections.
 */
public interface CommunicationManager {

	public <T> T readObject(int machineID) throws IOException, ClassNotFoundException;

	public void writeObject(int machineID, Object obj) throws IOException;

	// blocking call
	public void connectMachines(int noOfmachines) throws IOException;

	// non blocking call
	public void connectMachines(long timeOut) throws IOException;

	public void closeAllConnections() throws IOException;

	public void closeConnection(int machineID) throws IOException;

	public boolean isConnected(int machineID);

	/**
	 * MachineID 0 is reserved for master node.
	 */
	public List<Integer> getConnectedMachineIDs();

}