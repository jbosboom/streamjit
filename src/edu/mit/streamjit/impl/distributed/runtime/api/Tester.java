/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Error er = Error.FILE_NOT_FOUND;
		AppStatus apSts = AppStatus.STOPPED;
		ByteArrayOutputStream byteAos = new ByteArrayOutputStream();

		ObjectOutputStream os = null;
		try {
			os = new ObjectOutputStream(byteAos);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			os.writeObject(apSts);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * try { os.writeInt(34345); } catch (IOException e) { e.printStackTrace(); }
		 */

		System.out.println(byteAos.toByteArray().length);
	}
}
