package edu.mit.streamjit.impl.compiler2;

/**
 * A factory for ConcreteStorage instances based on Storage instances.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/28/2013
 */
public interface StorageFactory {
	public ConcreteStorage make(Storage storage);
}
