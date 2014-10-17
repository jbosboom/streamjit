/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * {@link NodeInfo} is to store and pass the information about the nodes such as
 * machines, servers, or mobile phones that are used to execute the stream
 * application. This informations may be needed by the {@link StreamNode}s in
 * order to establish connections with other dependent computing nodes to
 * successfully execute a stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 7, 2013
 */
public class NodeInfo implements SNMessageElement {

	private static final long serialVersionUID = -2627560179074739731L;

	/**
	 * Creates a {@link NodeInfo} and returns. This is added to ease the
	 * {@link NodeInfo} construction.
	 * 
	 * @return {@link NodeInfo} of the current computing node.
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
			NodeInfo myInfo = new NodeInfo(hostName, localMachine,
					availableCores, ramSize);
			return myInfo;
		} catch (UnknownHostException e) { // This exception is very unlikely to
											// be caught as we will have already
											// established the
											// Connection.
			e.printStackTrace();
		}
		// FIXME: Is this accepted programming practice?
		return null;
	}

	/**
	 * Human convenient hostName of the computing node. e.g, ClusterNode7,
	 * SamsungS2, testPC, etc. For easy logging and error printing.
	 */
	private String hostName;

	private InetAddress ipAddress;

	private int availableCores;

	/**
	 * RAM size in bytes.
	 */
	private long ramSize;

	public NodeInfo(String hostName, InetAddress ipAddress, int availableCores,
			long ramSize) {

		this.hostName = hostName;
		this.ipAddress = ipAddress;
		this.availableCores = availableCores;
		this.ramSize = ramSize;
	}

	/**
	 * As implementing {@link Cloneable} interface is greatly discouraged, lets
	 * use Copy constructor. No deep copy as all parameters are either primitive
	 * or immutable.
	 * 
	 * @param nodeinfo
	 *            : The {@link NodeInfo} object which need to be copied/cloned.
	 */
	public NodeInfo(NodeInfo nodeinfo) {
		this(nodeinfo.hostName, nodeinfo.ipAddress, nodeinfo.availableCores,
				nodeinfo.ramSize);
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
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public interface NodeInfoProcessor {
		public void process(NodeInfo nodeInfo);
	}
}
