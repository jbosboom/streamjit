package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * This class carries the Json string of a {@link Configuration} object.
 * {@link Controller} sends the json string to {@link StreamNode} with all
 * information of a stream application.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class ConfigurationString implements CTRLRMessageElement {

	private static final long serialVersionUID = -5900812807902330853L;

	private final String jsonString;
	private final ConfigType type;
	private final DrainData drainData;

	public ConfigurationString(String jsonString, ConfigType type,
			DrainData drainData) {
		this.jsonString = jsonString;
		this.type = type;
		this.drainData = drainData;
	}

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public void process(ConfigurationProcessor jp) {
		jp.process(jsonString, type, drainData);
	}

	/**
	 * Processes configuration string of a {@link Configuration} that is sent by
	 * {@link Controller}.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public interface ConfigurationProcessor {

		public void process(String cfg, ConfigType type, DrainData drainData);

		/**
		 * Indicates the type of the configuration.
		 */
		public enum ConfigType {
			/**
			 * Static configuration contains all details that is fixed for a
			 * StreamJit app and the given connected nodes.
			 */
			STATIC, /**
			 * Dynamic configuration contains all details that varies
			 * for each opentuner's new configuration.
			 */
			DYNAMIC
		}
	}
}
