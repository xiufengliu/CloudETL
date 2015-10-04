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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.logging.Logger;

import dk.aau.cs.cloudetl.common.Utils;

public class ServerThread implements Runnable {
	Logger log = Logger.getLogger(ServerThread.class.getName());

	private ByteChannel channel;
	private ByteBuffer buffer;
	private MetaStore metadata;

	private static ServerCmd[] commands = ServerCmd.values();

	public ServerThread(ByteChannel channel, MetaStore sda) {
		this.channel = channel;
		this.metadata = sda;
		this.buffer = ByteBuffer.allocate(4);
		//System.out.println("Establish connection!");
	}

	private final ServerCmd getCommand() throws IOException {
		buffer.clear();
		int read = Utils.read(channel, 4, buffer);
		if (read == -1)
			return null;
		int val = buffer.getInt(0);
		ServerCmd cmd;

		if (val >= 0 && val < commands.length)
			cmd = commands[val];
		else
			cmd = ServerCmd.BYE;
		return cmd;
	}

	@Override
	public void run() {
		try {
			while (!Thread.interrupted()) {
				ServerCmd cmd = getCommand();
				if (cmd==null) continue;
				switch (cmd) {
				case BYE:
					return;
				case READ_NEXT_SEQ:
					readNextSeq();
					break;
				default:
					throw new RuntimeException("Unknown command: " + cmd);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void readNextSeq() throws IOException {
		String seqName = Utils.readString(channel, buffer);
		
		int nextSeq = metadata.getNextSeq(seqName);
		//System.out.printf("%s=%d\n", seqName, nextSeq);
		
		buffer.clear();
		buffer.putInt(nextSeq);
		buffer.flip();
		channel.write(buffer);
	}

}
