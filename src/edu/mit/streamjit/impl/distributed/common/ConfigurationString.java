package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor.ConfigType;
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
public class ConfigurationString implements MessageElement {

	private static final long serialVersionUID = -5900812807902330853L;

	private String jsonString;
	private ConfigType type;

	public ConfigurationString(String jsonString, ConfigType type) {
		this.jsonString = jsonString;
		this.type = type;
	}

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}

	public void process(ConfigurationStringProcessor jp) {
		jp.process(jsonString, type);
	}

	/**
	 * Processes configuration string of a {@link Configuration} that is sent by
	 * {@link Controller}.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public interface ConfigurationStringProcessor {

		public void process(String cfg, ConfigType type);

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
