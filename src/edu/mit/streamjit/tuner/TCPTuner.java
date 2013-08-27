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

public final class TCPTuner implements AutoTuner {

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
		this.tuner = new ProcessBuilder("xterm", "-e", "python", tunerPath,
				port.toString()).start();
		this.connection = new TunerConnection();
		connection.connect(port);
	}

	@Override
	public void stopTuner() throws IOException {
		if (tuner == null)
			return;
		if (connection != null && connection.isStillConnected())
			connection.writeLine("exit");
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
			System.out.println("Going to make a connection with the server...");
			while (true) {
				try {
					socket = new Socket("localhost", port);
					System.out.println("Client connected...");
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

		AutoTuner tuner = new TCPTuner();
		try {
			tuner.startTuner("/home/sumanan/temp/Python/Socket/streamjit.py");
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
