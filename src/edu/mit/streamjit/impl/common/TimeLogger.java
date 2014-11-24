package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.OnlineTuner;

/**
 * Logs various time measurements for off line performance analysis. Controller
 * node can measure the time durations for different events and use this
 * interface to log those values.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Nov 22, 2014
 */
public interface TimeLogger {

	/**
	 * Log the total compilation time of a new configuration. (Controller node
	 * point of view).
	 * 
	 * @param time
	 */
	public void logCompileTime(long time);

	/**
	 * Writes additional messages to compileTime OutputStreamWriter.
	 * SNTimeInfoProcessor may use this method to log additional compilation
	 * messages those are collected from {@link StreamNode}s.
	 * 
	 * @param msg
	 */
	public void logCompileTime(String msg);

	/**
	 * Log total {@link DrainData} collection time.
	 * 
	 * @param time
	 */
	public void logDrainDataCollectionTime(long time);

	/**
	 * Log total draining time.
	 * 
	 * @param time
	 */
	public void logDrainTime(long time);

	/**
	 * Writes additional messages to drainTime OutputStreamWriter.
	 * SNTimeInfoProcessor may use this method to log additional draining
	 * messages those are collected from {@link StreamNode}s.
	 * 
	 * @param msg
	 */
	public void logDrainTime(String msg);

	/**
	 * Log the time taken to generate fixed amount of steady state outputs.
	 * 
	 * @param time
	 */
	public void logRunTime(long time);

	/**
	 * Writes additional messages to runTime OutputStreamWriter.
	 * SNTimeInfoProcessor may use this method to log additional runTime
	 * messages those are collected from {@link StreamNode}s.
	 * 
	 * @param msg
	 */
	public void logRunTime(String msg);

	/**
	 * This method shall be called to indicate the logger that a new
	 * configuration has been received. Appropriate caller would be
	 * {@link OnlineTuner}.
	 */
	public void newReconfiguration();
}
