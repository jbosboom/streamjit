package edu.mit.streamjit.tuner;

import java.io.IOException;

/**
 * Generic interface to communicate with Autotuner. Communication medium can be
 * anything. Standard In/Out, TCP or Native memory mapped buffers could be
 * considered.
 * 
 * @author sumanan
 * @since 8/26/2013
 */
public interface AutoTuner {

	/**
	 * Reads a line from the tuner.
	 * 
	 * @return Read string
	 */
	public String readLine() throws IOException;

	/**
	 * Writes the messages to the tuner.
	 * 
	 * @param message
	 * @return the bytes written
	 */
	public int writeLine(String message) throws IOException;

	/**
	 * @return <tt>true</tt> iff connection with the Autotuner is valid.
	 */
	public boolean isAlive();

	/**
	 * Starts the Autotuner instance.
	 * 
	 * @throws IOException
	 */
	public void startTuner(String tunerPath) throws IOException;

	/**
	 * Stop the Autotuner instance.
	 * 
	 * @throws IOException
	 */
	public void stopTuner() throws IOException;

}