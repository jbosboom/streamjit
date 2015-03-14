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
package edu.mit.streamjit.impl.distributed.common;

/**
 * {@link SystemInfo} holds the current system parameters such as CPU usage,
 * memory usage and battery level. Note that {@link NodeInfo} , in contrast to
 * {@link SystemInfo}, holds the computing node's hardware parameters such as IP
 * address, human readable name, CPU cores, RAM size, etc.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public class SystemInfo implements SNMessageElement {
	/**
	 * 
	 */
	private static final long serialVersionUID = 626480245760997626L;

	public double cpuUsage;
	public double memoryUsage;
	public double baterryLevel;

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public interface SystemInfoProcessor {
		public void process(SystemInfo systemInfo);
	}
}
