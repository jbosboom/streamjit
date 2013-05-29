/**
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *assigns machine id and keeps the set of connections. 
 */
public interface CommunicationManager {

	public <T> T readObject(int machineID, Class<T> klass) throws IOException, ClassNotFoundException;

	public boolean writeObject(int machineID, Object obj) throws IOException;

	//blocking call
	public boolean connectMachines(int noOfmachines) throws IOException;
	
	// non blocking call
	public boolean connectMachines(long timeOut) throws IOException;

	public boolean closeAllConnections() throws IOException;

	public boolean closeConnection(int machineID) throws IOException;

	public boolean isConnected(int machineID);
	
	/**
	 *  MachineID 0 is reserved for master node.
	 */
	public List<Integer> getConnectedMachineIDs();

	/**
	 *  Include Master's coreCount also. Blocking call.
	 */
	public Map<Integer, Integer> getCoreCount();
}