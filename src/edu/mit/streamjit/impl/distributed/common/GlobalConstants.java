package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.TailChannels;
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
	 * Decides how to start the opentuner. In first 2 cases, controller starts
	 * opentuner and establishes connection with it on a random port no range
	 * from 5000-65536. User can provide port no in 3 case.
	 * 
	 * <ol>
	 * <li>0 - Controller starts the tuner automatically on a terminal. User can
	 * see Opentuner related outputs in the new terminal.
	 * <li>1 - Controller starts the tuner automatically as a Python process. No
	 * explicit window will be opened. Suitable for remote running through SSH
	 * terminal.
	 * <li>2 - User has to manually start the tuner with correct portNo as
	 * argument. Port no 12563 is used in this case. But it can be changed at
	 * {@link TCPTuner#startTuner(String)}. We need this option to run the
	 * tuning on remote machines.
	 * </ol>
	 */
	public static final int tunerStartMode = 0;

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
	 * into FixedOutPut.txt. See {@link TailChannels} for the file logging
	 * details.
	 * <ol>
	 * 0 - No tuning, uses configuration file to run.
	 * <ol>
	 * 1 - Tuning.
	 * <ol>
	 * 2 - Evaluate configuration files. ( compares final cfg with hand tuned
	 * cfg. Both file should be presented in the running directory.
	 */
	public static final int tune = 1;

	/**
	 * Save all configurations tired by open tuner in to
	 * "configurations//app.name" directory.
	 */
	public static final boolean saveAllConfigurations = true;

	/**
	 * Output count for tuning. Tuner measures the running time for this number
	 * of outputs.
	 */
	public static final int outputCount = 100000;

	/**
	 * if true uses Compiler2, interpreter otherwise.
	 */
	public static final boolean useCompilerBlob = true;

	/**
	 * Period to print output count periodically. This printing feature get
	 * turned off if this value is less than 1. Time unit is ms. See
	 * {@link TailChannels}.
	 */
	public static final int printOutputCountPeriod = 6000;

	/**
	 * Enables {@link DistributedStreamCompiler} to run on a single node. When
	 * this is enabled, noOfNodes passed as compiler argument has no effect.
	 */
	public static final boolean singleNodeOnline = true;

	/**
	 * We can set this value at class loading time also as follows.
	 * 
	 * <code>maxThreadCount = Math.max(Runtime.getntime().availableProcessors() / 2,
	 * 1);</code>
	 * 
	 * Lets hard code this for the moment.
	 */
	public static final int maxNumCores = 24;

	/**
	 * Turn On/Off the profiling.
	 */
	public static final boolean needProfiler = true;
}
