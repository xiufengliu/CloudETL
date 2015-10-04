/*
 *
 * Copyright (c) 2011, Xiufeng Liu (xiliu@cs.aau.dk) and the eGovMon Consortium
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */

package dk.aau.cs.cloudetl.metadata;

import java.io.Console;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import dk.aau.cs.cloudetl.common.CEConstants;

public class MetaServer implements Runnable {
	private static final Log log = LogFactory.getLog(MetaServer.class);

	private static final long ACCEPTTIMEOUT = 1L;

	private static final long THREADPOOLSHUTDOWNTIMEOUT = 10L;

	private static final int THREADPOOLSIZE = 1000;

	private AtomicBoolean isRunning = new AtomicBoolean(false);
	private AtomicBoolean isStarted = new AtomicBoolean(false);

	private Thread thread;

	private ExecutorService executorService;

	private ServerSocketChannel channel;

	MetaStore metadata;
	

	public MetaServer(Configuration conf) {
		try {
			String hostname = InetAddress.getLocalHost().getHostName();
			conf.set("cloudetl.meta.server.host", hostname);
			conf.setInt("cloudetl.meta.server.port", CEConstants.SEQ_SERVER_PORT);
			this.thread = new Thread(this);
			this.metadata = new MetaStore(conf);
			//this.executorService = Executors.newFixedThreadPool(THREADPOOLSIZE);
			this.executorService = Executors.newCachedThreadPool();
			this.channel = ServerSocketChannel.open();
			this.channel.socket().bind(new InetSocketAddress(CEConstants.SEQ_SERVER_PORT));
			this.channel.configureBlocking(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void start() {
		if (isStarted.compareAndSet(false, true)) {
			isRunning.set(true);
			thread.start();
		} else
			throw new IllegalStateException("Receiver cannot be restarted.");
	}

	public void run() {
			while (isRunning.get()) {
			try {
				SocketChannel client = channel.accept();
				if (isRunning.get()) {
					executorService.submit(new ServerThread(client, metadata));
				}
			} catch (Exception ex) {
				if (!isRunning.get()) {
					return;
				}
			}
		}
	}

	public void cease() {
		if (!isStarted.get())
			return;
		
		metadata.materialize();
		
		if (isRunning.compareAndSet(true, false)) {
			try {
				thread.join(1 * TimeUnit.SECONDS.toMillis(ACCEPTTIMEOUT));
				if (thread.isAlive()) {
					log.warn("Metadata server is not dying gracefully; interrupting.");
					thread.interrupt();
				}
				thread.join(1 * TimeUnit.SECONDS.toMillis(ACCEPTTIMEOUT));
				if (thread.isAlive()) {
					log.fatal("Metadata server is not dying gracefully despite our insistence!.");
				}
				executorService.shutdown();
				if (!executorService.awaitTermination(
						THREADPOOLSHUTDOWNTIMEOUT, TimeUnit.SECONDS)) {
					executorService.shutdownNow();
				}
			} catch (InterruptedException ie) {
				log.warn("Interrupted during stopping: ", ie);
			}

			executorService.shutdown();
			log.info("Metadata server was stopped");
			executorService = null;
			thread = null;
			channel = null;
		}
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));
		conf.addResource(new Path("conf/cloudetl.xml"));
		
		MetaServer server = new MetaServer(conf);
		server.start();
		Console c = System.console();
		while (!"exit".equals(c.readLine("Sequence Server was started!\nEnter exit to stop: ")));
		server.cease();
	}
}