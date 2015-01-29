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

}
