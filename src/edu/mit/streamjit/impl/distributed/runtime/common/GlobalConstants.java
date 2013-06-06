/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.common;

/**
 * This class is to keep track of all application level constants. So we can avoid magical arbitrary values in other classes.
 */
public final class GlobalConstants {
	
	private GlobalConstants()
	{
		// No instantiation...
	}

	public static final int PORTNO = 39810;

	public static final String outterClassName = "outterClassName";
	public static final String topLevelWorkerName = "topLevelWorkerName";
	public static final String jarFilePath = "jarFilePath";
}
