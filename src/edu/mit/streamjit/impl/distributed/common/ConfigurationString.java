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

import edu.mit.streamjit.impl.blob.DrainData;
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

	public void process(ConfigurationStringProcessor jp) {
		jp.process(jsonString, type, drainData);
	}

	/**
	 * Processes configuration string of a {@link Configuration} that is sent by
	 * {@link Controller}.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public interface ConfigurationStringProcessor {

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
