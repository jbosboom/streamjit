package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.DrainData;
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
	 * This method shall be called to indicate the logger that a new
	 * configuration has been received. Appropriate caller would be
	 * {@link OnlineTuner}.
	 */
	public void newReconfiguration();

	/**
	 * Log the total compilation time of a new configuration. (Controller node
	 * point of view).
	 * 
	 * @param time
	 */
	public void logCompileTime(long time);

	/**
	 * Log the time taken to generate fixed amount of steady state outputs.
	 * 
	 * @param time
	 */
	public void logRunTime(long time);

	/**
	 * Log total draining time.
	 * 
	 * @param time
	 */
	public void logDrainTime(long time);

	/**
	 * Log total {@link DrainData} collection time.
	 * 
	 * @param time
	 */
	public void logDrainDataCollectionTime(long time);

}
