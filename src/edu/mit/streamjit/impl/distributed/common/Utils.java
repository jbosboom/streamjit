package edu.mit.streamjit.impl.distributed.common;

import java.util.Collections;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class Utils {

	public static Token getBlobID(Blob b) {
		return Collections.min(b.getInputs());
	}
}
