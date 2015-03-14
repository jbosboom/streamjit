package edu.mit.streamjit.impl.distributed.profiler;

import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement.SNProfileElementProcessor;

/**
 * Logs the {@link SNProfileElement}s. This interface extends
 * {@link SNProfileElementProcessor} so that there is no need to manually check
 * and add the process methods whenever a new {@link SNProfileElement} is added.
 * 
 * @author sumanan
 * @since 29 Jan, 2015
 */
public interface ProfileElementLogger extends SNProfileElementProcessor {

	/**
	 * This method shall be called to indicate to the logger that the
	 * configuration has been changed.
	 * 
	 * @param cfgName
	 *            The name of the new configuration. Pass an empty String if the
	 *            cfgName is not available (unknown) to the caller.
	 */
	public void newConfiguration(String cfgName);
}
