/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.common;

/**
 * This class is to keep track of all application level constants. So we can avoid magical arbitrary values in other classes.
 */
public final class GlobalConstants {

	private GlobalConstants() {
		// No instantiation...
	}

	public static final int PORTNO = 39810;

	public static final String OUTTER_CLASS_NAME = "OUTTER_CLASS_NAME";
	public static final String TOPLEVEL_WORKER_NAME = "TOPLEVEL_WORKER_NAME";
	public static final String JARFILE_PATH = "JARFILE_PATH";
	public static final String NODE_INFO_MAP = "nodeInfoMap";
	public static final String TOKEN_MACHINE_MAP = "tokenMachineMap";
	public static final String PORTID_MAP = "portIdMap";
	public static final String PARTITION = "partition";

}
