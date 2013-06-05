package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;

import edu.mit.streamjit.impl.distributed.runtime.slave.Slave;

/**
 * {@link NodeInfo} is to store and pass the information about the nodes such as machines, servers, or mobile phones that are used to
 * execute the stream application. This informations may be needed by the slaves in order to establish connections with other dependent
 * computing nodes to successfully execute a stream application.
 * 
 * {@link NodeInfo} keeps the {@link Socket} information as well. This object can be sent to {@link Slave}s to establish the connection
 * with the other {@link Slave}s to execute a stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 7, 2013
 */
public class NodeInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2627560179074739731L;

	/**
	 * Human convenient name of the computing node. e.g, ClusterNode7, SamsungS2, testPC, etc. For easy logging and error printing.
	 */
	private String name;

	private InetSocketAddress inetSktAddress;

	private int availableCores;

	/**
	 * {@link DistributedStreamCompiler} or {@link Master} may assign machindID to keep track of the slaves for the later processing.
	 */
	private int machineID;

	public NodeInfo(String name, InetSocketAddress inetSktAddress, int availableCores, int machineID) {
		this.name = name;
		this.inetSktAddress = inetSktAddress;
		this.availableCores = availableCores;
		this.setMachineID(machineID);
	}

	public NodeInfo(InetSocketAddress inetSktAddress, int machineID) {
		this.inetSktAddress = inetSktAddress;
		this.setMachineID(machineID);
	}

	/**
	 * As implementing {@link Cloneable} interface is greatly discouraged, lets use Copy constructor. The argument 2,
	 * {@link InetSocketAddress} has not been deep copied here as it is an immutable object.
	 * 
	 * @param nodeinfo
	 *            : The {@link NodeInfo} object which need to be copied/cloned.
	 */
	public NodeInfo(NodeInfo nodeinfo) {
		this(nodeinfo.name, nodeinfo.inetSktAddress, nodeinfo.availableCores, nodeinfo.machineID);
	}

	/**
	 * @return the name of human readable Name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the inetSktAddress
	 */
	public InetSocketAddress getInetSktAddress() {
		return inetSktAddress;
	}

	/**
	 * @param inetSktAddress
	 *            the inetSktAddress to set
	 */
	public void setInetSktAddress(InetSocketAddress inetSktAddress) {
		this.inetSktAddress = inetSktAddress;
	}

	/**
	 * This method was added to easily clone a {@link NodeInfo} and change the portNo so that the new cloned object can represent a new
	 * TCP socket.
	 * 
	 * @param portNo
	 */
	public void setPortNo(int portNo) {
		String hostname = this.inetSktAddress.getHostName();
		// InetSocketAddress is an immutable object. so we need to create a new one.
		this.inetSktAddress = new InetSocketAddress(hostname, portNo);
	}

	/**
	 * @return the availableCores
	 */
	public int getAvailableCores() {
		return availableCores;
	}

	/**
	 * @param availableCores
	 * 
	 */
	public void setAvailableCores(int availableCores) {
		this.availableCores = availableCores;
	}

	/**
	 * @return : ID of this node.
	 */
	public int getMachineID() {
		return machineID;
	}

	/**
	 * @param machineID
	 */
	public void setMachineID(int machineID) {
		this.machineID = machineID;
	}
}
