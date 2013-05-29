package edu.mit.streamjit.impl.distributed;

import java.net.InetSocketAddress;

import sun.net.util.IPAddressUtil;

/**
 * {@link ComputingNodeInfo} is to store and pass the information about the nodes such as machines, servers, or mobile phones that are used to
 * execute the stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 7, 2013
 */
public class ComputingNodeInfo {

	private String name;	
	private InetSocketAddress inetSktAddress;
	private int availableCores;
	
	public ComputingNodeInfo(String name,InetSocketAddress inetSktAddress, int availableCores)
	{
		this.name = name;
		this.inetSktAddress = inetSktAddress;
		this.availableCores = availableCores;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
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
	 * @param inetSktAddress the inetSktAddress to set
	 */
	public void setInetSktAddress(InetSocketAddress inetSktAddress) {
		this.inetSktAddress = inetSktAddress;
	}

	/**
	 * @return the availableCores
	 */
	public int getAvailableCores() {
		return availableCores;
	}

	/**
	 * @param availableCores the availableCores to set
	 */
	public void setAvailableCores(int availableCores) {
		this.availableCores = availableCores;
	}	
}
