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
package edu.mit.streamjit.tuner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

import edu.mit.streamjit.impl.distributed.common.GlobalConstants;

public final class TCPTuner implements OpenTuner {

	Process tuner;
	TunerConnection connection;

	@Override
	public String readLine() throws IOException {
		if (connection == null)
			throw new AssertionError(
					"Connection with the Autotuner is not established yet.");
		return connection.readLine();
	}

	@Override
	public int writeLine(String message) throws IOException {
		if (connection == null)
			throw new AssertionError(
					"Connection with the Autotuner is not established yet.");
		connection.writeLine(message);
		return message.length();
	}

	@Override
	public boolean isAlive() {
		if (tuner == null)
			return false;
		try {
			tuner.exitValue();
			return false;
		} catch (IllegalThreadStateException ex) {
			return true;
		}
	}

	@Override
	public void startTuner(String tunerPath) throws IOException {
		int min = 5000;
		Random rand = new Random();
		Integer port = rand.nextInt(65535 - min) + min;
		if (GlobalConstants.tunerMode == 0) {
			this.tuner = new ProcessBuilder("xterm", "-e", "python", tunerPath,
					port.toString()).start();
		} else
			port = 12563;
		this.connection = new TunerConnection();
		connection.connect(port);
	}

	@Override
	public void stopTuner() throws IOException {
		if (tuner == null)
			return;
		if (connection != null && connection.isStillConnected())
			connection.writeLine("exit\n");
		else
			tuner.destroy();

		try {
			tuner.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (connection != null && !isAlive()) {
			connection.closeConnection();
		}
	}

	/**
	 * Communicate with the Autotuner over the local host TCP connection.
	 * 
	 * @author sumanan
	 * 
	 */
	private final class TunerConnection {

		private BufferedReader reader = null;
		private BufferedWriter writter = null;
		private Socket socket = null;
		private boolean isconnected = false;

		void connect(int port) throws IOException {
			Socket socket;
			while (true) {
				try {
					socket = new Socket("localhost", port);
					System.out.println("Autotuner is connected...");
					this.socket = socket;

					InputStream is = socket.getInputStream();
					OutputStream os = socket.getOutputStream();
					this.reader = new BufferedReader(new InputStreamReader(is));
					this.writter = new BufferedWriter(
							new OutputStreamWriter(os));
					isconnected = true;
					break;

				} catch (UnknownHostException e) {
					throw e;
				} catch (IOException e) {
					System.out
							.println("Trying to make TCP connection with Autotuner. It seems Autotuner is not up yet.");

					// Lets wait for a second before attempting next.
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		}

		public void writeLine(String msg) throws IOException {
			if (isStillConnected()) {
				try {
					writter.write(msg);
					if (msg.toCharArray()[msg.length() - 1] != '\n')
						writter.write('\n');
					writter.flush();
				} catch (IOException ix) {
					isconnected = false;
					throw ix;
				}
			} else {
				throw new IOException("TCPConnection: Socket is not connected");
			}
		}

		String readLine() throws IOException {
			if (isStillConnected()) {
				try {
					return reader.readLine();
				} catch (IOException e) {
					isconnected = false;
					throw e;
				}
			} else {
				throw new IOException("TCPConnection: Socket is not connected");
			}
		}

		public final void closeConnection() {
			isconnected = false;
			try {
				if (reader != null)
					this.reader.close();
				if (writter != null)
					this.writter.close();
				if (socket != null)
					this.socket.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		boolean isStillConnected() {
			return isconnected;
		}
	}

	public static void main(String[] args) throws InterruptedException,
			IOException {

		OpenTuner tuner = new TCPTuner();
		try {
			tuner.startTuner("/lib/opentuner/streamjit/streamjit.py");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Thread.sleep(1000);
		try {
			tuner.stopTuner();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
