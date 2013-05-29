/**
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.api;


public class SystemInfo implements MessageElement{
	public double cpuUsage;
	public double memoryUsage;
	public double baterryLevel;
	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}	
}
