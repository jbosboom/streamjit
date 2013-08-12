package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.MessageElement;
import edu.mit.streamjit.impl.distributed.common.MessageVisitor;
import edu.mit.streamjit.impl.distributed.common.MessageVisitorImpl;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
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

	/**
	 * StreamNodeAgent represents a {@link StreamNode} at {@link Controller}
	 * side. Controller will be having a StreamNodeAgent for each connected
	 * StreamNode.
	 * <p>
	 * IO connection is not part of the StreamNodeAgent and StreamNodeAgent is
	 * expected to be a passive object. This decision is made because single
	 * thread or a dispatcher thread pool is expected to be running for
	 * asynchronous IO or non blocking IO implementation of the
	 * {@link CommunicationManager}. It is {@link CommunicationManager}'s
	 * responsibility to run the IO connections of each {@link StreamNode} on
	 * appropriate thread. If it is blocking IO then each communication can be
	 * run on separate thread. Otherwise, single thread or a thread pool may
	 * handle all IO communications. IO threads will read the incoming messages
	 * and act accordingly. Controller thread also can send messages to
	 * {@link StreamNode}s in parallel.
	 * </p>
	 */
	public static abstract class StreamNodeAgent {

		/**
		 * {@link MessageVisitor} for this streamnode.
		 */
		private final MessageVisitor mv;

		/**
		 * Assigned nodeID of the corresponding {@link StreamNode}.
		 */
		private final int nodeID;

		/**
		 * Recent {@link AppStatus} from the corresponding {@link StreamNode}.
		 */
		private volatile AppStatus appStatus;

		/**
		 * {@link NodeInfo} of the {@link StreamNode} that is mapped to this
		 * {@link StreamNodeAgent}.
		 */
		private volatile NodeInfo nodeInfo;

		/**
		 * Recent {@link Error} message from the corresponding
		 * {@link StreamNode}.
		 */
		private volatile Error error;

		/**
		 * Recent {@link SystemInfo} from the corresponding {@link StreamNode}.
		 */
		private volatile SystemInfo systemInfo;

		/**
		 * Stop the communication with corresponding {@link StreamNode}.
		 * {@link Controller} is expected to set this flag and the IO thread
		 * response for that.
		 */
		private AtomicBoolean stopFlag;

		public StreamNodeAgent(int nodeID) {
			this.nodeID = nodeID;
			stopFlag = new AtomicBoolean(false);
			mv = new MessageVisitorImpl(new CNAppStatusProcessorImpl(this),
					new CNCommandProcessorImpl(),
					new CNErrorProcessorImpl(this),
					new CNRequestProcessorImpl(),
					new CNCfgStringProcessorImpl(), new CNDrainProcessorImpl(),
					new CNNodeInfoProcessorImpl(this));
		}

		/**
		 * @return the nodeID
		 */
		public int getNodeID() {
			return nodeID;
		}

		/**
		 * @return the appStatus
		 */
		public AppStatus getAppStatus() {
			return appStatus;
		}

		/**
		 * @param appStatus
		 *            the appStatus to set
		 */
		public void setAppStatus(AppStatus appStatus) {
			this.appStatus = appStatus;
		}

		/**
		 * @return the nodeInfo
		 */
		public NodeInfo getNodeInfo() {
			if (nodeInfo == null) {
				try {
					writeObject(Request.NodeInfo);
				} catch (IOException e) {
					e.printStackTrace();
				}
				// TODO: If in any chance the IO thread call this function then
				// it will get blocked on this loop forever. Need to handle
				// this.
				while (nodeInfo == null) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			return nodeInfo;
		}

		/**
		 * @param nodeInfo
		 *            the nodeInfo to set
		 */
		public void setNodeInfo(NodeInfo nodeInfo) {
			this.nodeInfo = nodeInfo;
		}

		/**
		 * @return the error
		 */
		public Error getError() {
			return error;
		}

		/**
		 * @param error
		 *            the error to set
		 */
		public void setError(Error error) {
			this.error = error;
		}

		/**
		 * @return the systemInfo
		 */
		public SystemInfo getSystemInfo() {
			return systemInfo;
		}

		/**
		 * @param systemInfo
		 *            the systemInfo to set
		 */
		public void setSystemInfo(SystemInfo systemInfo) {
			this.systemInfo = systemInfo;
		}

		/**
		 * Stop the communication with corresponding {@link StreamNode}.
		 */
		public void stopRequest() {
			this.stopFlag.set(true);
		}

		public boolean isStopRequested() {
			return this.stopFlag.get();
		}

		/**
		 * @return Human readable name of the streamNode.
		 */
		public String streamNodeName() {
			if (nodeInfo == null) {
				getNodeInfo();
			}
			return nodeInfo.getHostName();
		}

		/**
		 * Send a object to corresponding {@link StreamNode}. While IO threads
		 * reading the incoming messages from the stream node, controller may
		 * send any messages through this function.
		 * 
		 * @param obj
		 * @throws IOException
		 */
		public abstract void writeObject(Object obj) throws IOException;

		/**
		 * Is still the connection with corresponding {@link StreamNode} is
		 * alive?
		 * 
		 * @return
		 */
		public abstract boolean isConnected();

		/**
		 * @return the mv
		 */
		public MessageVisitor getMv() {
			return mv;
		}
	}
}