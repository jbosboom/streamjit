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

import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.TailChannel;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.StreamNodeAgent;
import edu.mit.streamjit.tuner.TCPTuner;

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
	 * The actual IP addresses of the nodes that the nodes use to make
	 * connection with the controller. We need this, because a node may have
	 * multiple network interfaces and those may have multiple IP addresses too.
	 * See the comment of {@link StreamNodeAgent#getAddress()}.
	 */
	public static final String INETADDRESS_MAP = "iNetAddressMap";

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
	public static final String CONINFOMAP = "ConInfoMap";

	/**
	 * Whether to start the tuner automatically or not.
	 * <ol>
	 * <li>0 - Controller will start the tuner automatically.
	 * <li>1 - User has to manually start the tuner with correct portNo as
	 * argument. Port no 12563 is used in this case. But it can be changed at
	 * {@link TCPTuner#startTuner(String)}. We need this option to run the
	 * tuning on remote machines.
	 * </ol>
	 */
	public static int tunerMode = 0;

	/**
	 * To turn on or turn off the drain data. If this is false, drain data will
	 * be ignored and every new reconfiguration will run with fresh inputs.
	 */
	public static final boolean useDrainData = false;

	/**
	 * To turn on or off the dead lock handler. see {@link AbstractDrainer} for
	 * it's usage.
	 */
	public static final boolean needDrainDeadlockHandler = true;

	/**
	 * Enables tuning. Tuner will be started iff this flag is set true.
	 * Otherwise, just use the fixed configuration file to run the program. No
	 * tuning, no intermediate draining. In this mode (tune = false), time taken
	 * to pass fixed number of input will be measured for 30 rounds and logged
	 * into FixedOutPut.txt. See {@link TailChannel} for the file logging
	 * details.
	 */
	public static final boolean tune = false;

	/**
	 * Save all configurations tired by open tuner in to
	 * "configurations//app.name" directory.
	 */
	public static final boolean saveAllConfigurations = true;

	static {

	}
}
