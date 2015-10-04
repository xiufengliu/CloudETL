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
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.CEConfigurable;
import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.Utils;

public class SEQ implements CEConfigurable, Serializable {
	private static final long serialVersionUID = -6406296187863415476L;

	String name;
	byte[] nameInBytes;
	int curSeq, endSeq;
	SocketChannel channel;
	ByteBuffer buffer;
	Configuration conf;

	public SEQ(String name) {
		try {
			this.name = name;
			this.nameInBytes = Utils.getBytesUtf8(name);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setup(TaskAttemptContext context) {
		try {
			this.conf = context.getConfiguration();
			String hostname = conf.get("cloudetl.meta.server.host");
			int port = conf.getInt("cloudetl.meta.server.port",	CEConstants.SEQ_SERVER_PORT);
			this.channel = SocketChannel.open(new InetSocketAddress(hostname,port));

			this.buffer = ByteBuffer.allocate(512);

			this.curSeq = this.readNextFromServer();
			this.endSeq = curSeq + conf.getInt(CEConstants.SEQ_INCR_DELTA, 10000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int readNextFromServer() throws IOException {
		buffer.clear();
		buffer.putInt(ServerCmd.READ_NEXT_SEQ.ordinal())
			  .putInt(nameInBytes.length).put(nameInBytes).flip();
		channel.write(buffer);

		buffer.clear();
		channel.read(buffer);
		return buffer.getInt(0);
	}

	public int nextSeq() throws IOException {
		if (curSeq >= endSeq) {
			this.curSeq = readNextFromServer();
			this.endSeq = this.curSeq + conf.getInt(CEConstants.SEQ_INCR_DELTA, 10000);
		}
		return curSeq++;
	}

	@Override
	public void cleanup(TaskAttemptContext context) {
		try {
			buffer.clear();
			buffer.putInt(ServerCmd.BYE.ordinal()).flip();
			channel.write(buffer);
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
	}

}
