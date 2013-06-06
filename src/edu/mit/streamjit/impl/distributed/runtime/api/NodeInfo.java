package edu.mit.streamjit.impl.distributed.runtime.api;

import edu.mit.streamjit.impl.distributed.runtime.common.Ipv4Validator;

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
	 * Human convenient name of the computing node. e.g, ClusterNode7, SamsungS2, testPC, etc. For easy logging and error printing.
	 */
	private String name;

	private String ipv4Address;

	private int availableCores;

	/**
	 * RAM size in MB.
	 */
	private int ramSize;

	public NodeInfo(String name, String ipv4Address, int availableCores, int ramSize) {
		if (!Ipv4Validator.getInstance().isValid(ipv4Address))
			throw new IllegalArgumentException("Invalid IPV4 address");

		this.name = name;
		this.setIpv4Address(ipv4Address);
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
		this(nodeinfo.name, nodeinfo.ipv4Address, nodeinfo.availableCores, nodeinfo.ramSize);
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
	 * @return RAM size in MB.
	 */
	public int getRjamSize() {
		return ramSize;
	}

	/**
	 * @param ramSize
	 *            : RAM size in MB.
	 */
	public void setRamSize(int ramSize) {
		this.ramSize = ramSize;
	}

	public String getIpv4Address() {
		return ipv4Address;
	}

	public void setIpv4Address(String ipv4Address) {
		this.ipv4Address = ipv4Address;
	}

	@Override
	public void accept(MessageVisitor visitor) {
		throw new AssertionError("NodeInfo doesn't support MessageVisitor for the moment.");
	}
}
