package edu.mit.streamjit.impl.distributed.profiler;

/**
 * Profiles a specific resources (e.g, buffer status of all blobs) of a
 * StreamNode.
 * 
 * @author sumanan
 * @since 26 Jan, 2015
 */
public interface StreamNodeProfiler {

	/**
	 * A profiler thread will call this method to get the current status of the
	 * resource that is being profiled. Implementation must be thread safe.
	 * 
	 * @return Current status of the resource that is being profiled.
	 */
	public SNProfileElement profile();

}