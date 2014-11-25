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
	 * Compiler may call this method to indicate the compilation event has
	 * started. TimeLogger may start a timer to measure the compilation time.
	 */
	public void compilationStarted();

	/**
	 * Compiler can call this method to indicate the compilation event has
	 * finished. TimeLogger may stop the timer and log the compilation time.
	 * 
	 * @param isCompiled
	 *            : Additional detail that goes with log.
	 * @param msg
	 *            : Additional details that go with log.
	 */
	public void compilationFinished(boolean isCompiled, String msg);

	/**
	 * Drainer or Tuner may call this method to indicate the draining even has
	 * started. TimeLogger may start a timer to measure the compilation time.
	 */
	public void drainingStarted();

	/**
	 * Drainer or Tuner may call this method to indicate the draining even has
	 * finished. TimeLogger may stop the timer and log the draining time.
	 * 
	 * @param msg
	 *            : Additional details that go with log.
	 */
	public void drainingFinished(String msg);

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
