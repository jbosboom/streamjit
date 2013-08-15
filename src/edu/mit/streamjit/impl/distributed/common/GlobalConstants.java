package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * This class is to keep track of all application level constants. So we can
 * avoid magical arbitrary values in other classes.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public final class GlobalConstants {

	private GlobalConstants() {
		// No instantiation...
	}

	public static final int PORTNO = 39810;

	public static final String TOPLEVEL_WORKER_NAME = "TOPLEVEL_WORKER_NAME";
	public static final String JARFILE_PATH = "JARFILE_PATH";
	/**
	 * nodeID to {@link NodeInfo} map is stored in the configuration in this
	 * name.
	 */
	public static final String NODE_INFO_MAP = "nodeInfoMap";
	/**
	 * tokenMachineMap that says mapped nodeID of upstream and downstream
	 * workers of a token is stored in the configuration in this name.
	 * tokenMachineMap is type of Map<Token, Map.Entry<Integer, Integer>> where
	 * value is a map entry. This map entry's Key - MachineID of the source
	 * node, Value - MachineID of the destination node.
	 */
	public static final String TOKEN_MACHINE_MAP = "tokenMachineMap";
	/**
	 * Information of TCP portID for a token whose upstream and downstream
	 * workers are mapped to different nodes in a distributed execution
	 * environment is stored in the configuration in this name.
	 * {@link StreamNode}s can use this information to establish
	 * {@link BoundaryChannel}s so that both workers can communicate.
	 */
	public static final String PORTID_MAP = "portIdMap";
	public static final String PARTITION = "partition";

}
