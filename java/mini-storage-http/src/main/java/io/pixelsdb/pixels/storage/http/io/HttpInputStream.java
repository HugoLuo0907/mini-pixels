/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.concurrent.*;

public class HttpInputStream extends InputStream
{
    private static final Logger logger = LogManager.getLogger(HttpInputStream.class);

    private boolean open;
    private final String schema = "http";
    private String host;
    private int port;
    private String uri;

    /**
     * 改为 LinkedBlockingQueue：纯 FIFO 场景下双端操作多余，
     * 且 put/take 分离锁可降低生产者-消费者锁竞争。
     */
    private final BlockingQueue<ByteBuf> contentQueue;

    /** 当前正在消费的 ByteBuf，避免每轮循环 peek() 加锁。 */
    private ByteBuf currentBuf;

    private final int MAX_TRIES = Constants.MAX_STREAM_RETRY_COUNT;
    private final long DELAY_MS = Constants.STREAM_DELAY_MS;

    private final HttpServer httpServer;
    private final ExecutorService executorService;
    private final CompletableFuture<Void> httpServerFuture;

    public HttpInputStream(String host, int port) throws CertificateException, SSLException {
        this.open = true;
        this.contentQueue = new LinkedBlockingQueue<>();
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.httpServer = new HttpServer(new StreamHttpServerHandler(this.contentQueue));
        this.executorService = Executors.newFixedThreadPool(1);
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try {
                this.httpServer.serve(this.port);
            } catch (InterruptedException e) {
                logger.error("http server interrupted", e);
            }
        }, this.executorService);
    }

    @Override
    public int read() throws IOException
    {
        assertOpen();
        if (this.currentBuf == null || !this.currentBuf.isReadable()) {
            if (this.currentBuf != null) {
                this.currentBuf.release();
                this.currentBuf = null;
            }
            this.currentBuf = pollContent();
            if (this.currentBuf == null) return -1;
        }
        return this.currentBuf.readUnsignedByte();
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        assertOpen();
        if (len == 0) return 0;

        int readBytes = 0;
        while (readBytes < len) {
            if (this.currentBuf == null || !this.currentBuf.isReadable()) {
                if (this.currentBuf != null) {
                    this.currentBuf.release();
                    this.currentBuf = null;
                }
                this.currentBuf = pollContent();
                if (this.currentBuf == null) {
                    return readBytes > 0 ? readBytes : -1;
                }
            }
            int readLen = Math.min(len - readBytes, this.currentBuf.readableBytes());
            this.currentBuf.readBytes(buf, off + readBytes, readLen);
            readBytes += readLen;
        }
        return readBytes;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open) {
            this.open = false;
            this.httpServerFuture.complete(null);
            this.httpServer.close();
            releaseResources();
        }
    }

    /** 释放 currentBuf 与队列中残留的 ByteBuf，防止 Netty 直接内存泄漏。 */
    private void releaseResources()
    {
        if (this.currentBuf != null) {
            this.currentBuf.release();
            this.currentBuf = null;
        }
        ByteBuf buf;
        while ((buf = this.contentQueue.poll()) != null) {
            buf.release();
        }
    }

    /** 使用队列的带超时 poll 替代固定 Thread.sleep 忙等待。 */
    private ByteBuf pollContent() throws IOException
    {
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos((long) this.MAX_TRIES * this.DELAY_MS);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    logger.error("retry count {}, httpServerFuture {}, exception cause: " +
                                    "HttpInputStream failed to receive data within {} ms",
                            this.MAX_TRIES, this.httpServerFuture.isDone(),
                            (long) this.MAX_TRIES * this.DELAY_MS);
                    return null;
                }
                ByteBuf buf = this.contentQueue.poll(remaining, TimeUnit.NANOSECONDS);
                if (buf != null) return buf;
                if (this.httpServerFuture.isDone()) {
                    return this.contentQueue.poll(); // 最终非阻塞 drain
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for data", e);
        }
    }

    private void assertOpen()
    {
        if (!this.open) throw new IllegalStateException("Closed");
    }

    public static class StreamHttpServerHandler extends HttpServerHandler
    {
        private static final Logger logger = LogManager.getLogger(StreamHttpServerHandler.class);
        private final BlockingQueue<ByteBuf> contentQueue;

        public StreamHttpServerHandler(BlockingQueue<ByteBuf> contentQueue)
        {
            this.contentQueue = contentQueue;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
        {
            if (!(msg instanceof HttpRequest)) return;
            FullHttpRequest req = (FullHttpRequest) msg;
            if (req.method() != HttpMethod.POST) {
                sendResponse(ctx, req, HttpResponseStatus.OK);
                return;
            }
            // 常量前置，避免 header 缺失时 NPE
            if (!"application/x-protobuf".equals(req.headers().get(HttpHeaderNames.CONTENT_TYPE))) {
                return;
            }
            ByteBuf content = req.content();
            if (content.isReadable()) {
                content.retain();
                this.contentQueue.add(content);
            }
            sendResponse(ctx, req, HttpResponseStatus.OK);
        }

        private void sendResponse(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status)
        {
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
            response.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                    .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            if (HttpHeaderValues.CLOSE.toString().equals(req.headers().get(HttpHeaderNames.CONNECTION))) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                response.setStatus(status);
                ctx.writeAndFlush(response);
                this.serverCloser.run();
            } else {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                response.setStatus(status);
                ctx.writeAndFlush(response);
            }
        }
    }
}