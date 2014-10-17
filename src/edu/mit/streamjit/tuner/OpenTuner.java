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
public interface OpenTuner {

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