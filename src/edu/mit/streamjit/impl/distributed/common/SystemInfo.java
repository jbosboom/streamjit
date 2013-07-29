package edu.mit.streamjit.impl.distributed.common;


/**
 * {@link SystemInfo} holds the current system parameters such as CPU usage, memory usage and battery level. Note that {@link NodeInfo}
 * , in contrast to {@link SystemInfo}, holds the computing node's hardware parameters such as IP address, human readable name, CPU
 * cores, RAM size, etc.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public class SystemInfo implements MessageElement {
	/**
	 * 
	 */
	private static final long serialVersionUID = 626480245760997626L;
	
	public double cpuUsage;
	public double memoryUsage;
	public double baterryLevel;

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}
	
	public interface SystemInfoProcessor {

	}
}
