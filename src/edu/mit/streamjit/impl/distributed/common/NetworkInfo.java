package edu.mit.streamjit.impl.distributed.common;

import java.net.InetAddress;
import java.util.Map;

/**
 * Keeps network information of all nodes in the system.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 23, 2014
 */
public class NetworkInfo {

	private final Map<Integer, InetAddress> iNetAddressMap;

	public NetworkInfo(Map<Integer, InetAddress> iNetAddressMap) {
		this.iNetAddressMap = iNetAddressMap;
	}

	public InetAddress getInetAddress(int nodeID) {
		if (this.iNetAddressMap == null)
			return null;
		InetAddress ipAddress = iNetAddressMap.get(nodeID);
		if (ipAddress.isLoopbackAddress())
			ipAddress = iNetAddressMap.get(0);
		return ipAddress;
	}
}
