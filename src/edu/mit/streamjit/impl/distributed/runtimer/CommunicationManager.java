package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.Map;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * {@link CommunicationManager} manages all type of communications. Its
 * responsible to establish the connection with {@link StreamNode}s and keep
 * track of all {@link StreamNodeAgent}s. Further, communication manager should
 * handle all IO threads as well.
 * <p>
 * Mainly two type of CommunicationManager is expected to be implemented.
 * <li>Blocking IO using java IO package. In this case each
 * {@link StreamNodeAgent} can be made to run on individual thread and keep on
 * reading for incoming {@link MessageElement} from the corresponding
 * {@link StreamNode} and process it.
 * <li>Non-blocking IO using java NIO package. In this case,
 * {@link CommunicationManager} should handle the dispatcher thread pool which
 * may select ready channels and process the send/receive operation.
 * </p>
 * TODO: Need to handle the connection loss and reconnection. Assigns nodeID
 * based on the connecting order.
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

	/**
	 * Establishes all connections based on the argument comTypeCount. But only
	 * one local connection can be created. So the value for the key
	 * {@link CommunicationType}.LOCAL in the argument comTypeCount has no
	 * effect. This is a blocking call.
	 * 
	 * @param comTypeCount
	 *            Map that tells the number of connections need to be
	 *            established for each {@link CommunicationType}.
	 * @throws IOException
	 * 
	 * @return A map in where the key is assigned nodeID of the connected
	 *         {@link StreamNode}s and value is {@link StreamNodeAgent}.
	 *         StreamNodeAgent is representative of streamnode at controller
	 *         side.
	 */
	public Map<Integer, StreamNodeAgent> connectMachines(
			Map<CommunicationType, Integer> comTypeCount) throws IOException;

	/**
	 * Close all connections. Further, it stops all threads and release all
	 * resources of this {@link CommunicationManager}.
	 * 
	 * @throws IOException
	 */
	public void closeAllConnections() throws IOException;
}