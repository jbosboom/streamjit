package edu.mit.streamjit.impl.distributed.api;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * {@link NodeInfo} is to store and pass the information about the nodes such as machines, servers, or mobile phones that are used to
 * execute the stream application. This informations may be needed by the slaves in order to establish connections with other dependent
 * computing nodes to successfully execute a stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 7, 2013
 */
public class NodeInfo implements MessageElement {

	private static final long serialVersionUID = -2627560179074739731L;

	/**
	 * As this code snippet become repeated in many places ( master side and slave side) , static methods is introduced to ease the
	 * job. No more do nodes need to calculate everything them self. instead, nodes can get their {@link NodeInfo} through this static
	 * method.
	 * 
	 * @return
	 */
	public static NodeInfo getMyinfo() {
		InetAddress localMachine;
		String hostName;
		int availableCores = Runtime.getRuntime().availableProcessors();
		// TODO: Need to verify the ramSize.
		long ramSize = Runtime.getRuntime().maxMemory();
		try {
			localMachine = InetAddress.getLocalHost();
			hostName = localMachine.getHostName();
			NodeInfo myInfo = new NodeInfo(hostName, localMachine, availableCores, ramSize);
			return myInfo;
		} catch (UnknownHostException e) { // This exception is very unlikely to be caught as we will have already established the
											// Connection.
			e.printStackTrace();
		}
		// FIXME: Is this accepted programming practice?
		return null;
	}

	/**
	 * Human convenient hostName of the computing node. e.g, ClusterNode7, SamsungS2, testPC, etc. For easy logging and error printing.
	 */
	private String hostName;

	private InetAddress ipAddress;

	private int availableCores;

	/**
	 * RAM size in bytes.
	 */
	private long ramSize;

	public NodeInfo(String hostName, InetAddress ipAddress, int availableCores, long ramSize) {

		this.hostName = hostName;
		this.ipAddress = ipAddress;
		this.availableCores = availableCores;
		this.ramSize = ramSize;
	}

	/**
	 * As implementing {@link Cloneable} interface is greatly discouraged, lets use Copy constructor. No deep copy as all parameters
	 * are either primitive or immutable.
	 * 
	 * @param nodeinfo
	 *            : The {@link NodeInfo} object which need to be copied/cloned.
	 */
	public NodeInfo(NodeInfo nodeinfo) {
		this(nodeinfo.hostName, nodeinfo.ipAddress, nodeinfo.availableCores, nodeinfo.ramSize);
	}

	/**
	 * @return the availableCores
	 */
	public int getAvailableCores() {
		return availableCores;
	}

	/**
	 * @return the hostName of human readable Name.
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @return the ipAddress
	 */
	public InetAddress getIpAddress() {
		return ipAddress;
	}

	/**
	 * @return RAM size in bytes
	 */
	public long getRamSize() {
		return ramSize;
	}

	@Override
	public void accept(MessageVisitor visitor) {
		throw new AssertionError("NodeInfo doesn't support MessageVisitor for the moment.");
	}
}
