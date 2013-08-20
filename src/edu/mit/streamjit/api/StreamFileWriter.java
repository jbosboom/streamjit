package edu.mit.streamjit.api;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import edu.mit.streamjit.test.sanity.streamfilereader.StreamFileReader;

/**
 * Writes the stream objects in into a file. Creates a new file and if the file is already available, overwrites it. ( Simply serialize
 * the stream objects into the file.)
 * 
 * Refer {@link StreamFileReader} to to read back the serialized output of this class. Read {@link StreamFileReader}'s comment as well.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 29, 2013
 */
public class StreamFileWriter<T extends Serializable> extends Filter<T, Void> {
	ObjectOutputStream objOut;

	public StreamFileWriter(String filePath) {
		super(1, 0);

		File f = new File(filePath);
		try {
			FileOutputStream fo = new FileOutputStream(f);
			objOut = new ObjectOutputStream(fo);
		} catch (FileNotFoundException e) {
			throw new AssertionError("File not found", e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	// TODO: Do we need to stop the execution of the stream graph if IOException occurred?
	public void work() {
		try {
			objOut.writeObject(pop());
		} catch (IOException e) {
			e.printStackTrace();
			// System.exit(1);
		}
	}
}