/*
 * Copyright (c) 2026, CodexCoder21Organization. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * @test
 * @summary Chunked response availability must use linear buffer allocation
 * @modules jdk.management
 * @run main/othervm ChunkedBufferGrowth
 */

import com.sun.management.ThreadMXBean;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ChunkedBufferGrowth {
    public static void main(String[] args) throws Exception {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        boolean previouslyEnabled = bean.isThreadAllocatedMemoryEnabled();
        bean.setThreadAllocatedMemoryEnabled(true);
        try {
            for (int size : new int[] {2048, 4096, 8192, 16384}) {
                checkResponse(bean, size);
            }
        } finally {
            bean.setThreadAllocatedMemoryEnabled(previouslyEnabled);
        }
    }

    private static void checkResponse(ThreadMXBean bean, int size) throws Exception {
        byte[] wire = ("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n"
                + "Connection: close\r\n\r\n"
                + "1\r\na\r\n".repeat(size) + "0\r\n\r\n")
                .getBytes(StandardCharsets.US_ASCII);
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket server = new ServerSocket(0, 1, loopback);
             var executor = Executors.newSingleThreadExecutor()) {
            var sent = executor.submit(() -> {
                try (var peer = server.accept()) {
                    peer.setSoTimeout(5000);
                    var request = new BufferedReader(new InputStreamReader(
                            peer.getInputStream(), StandardCharsets.US_ASCII));
                    String line;
                    while ((line = request.readLine()) != null && !line.isEmpty()) { }
                    peer.getOutputStream().write(wire);
                    peer.getOutputStream().flush();
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            });
            URI uri = new URI("http", null, loopback.getHostAddress(),
                    server.getLocalPort(), "/", null, null);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setReadTimeout(5000);
            connection.setConnectTimeout(5000);
            try {
                if (connection.getResponseCode() != 200) {
                    throw new AssertionError("Expected HTTP 200");
                }
                // Complete the valid framed response before asking what is available.
                sent.get(5, TimeUnit.SECONDS);
                try (var input = connection.getInputStream()) {
                    long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
                    int available = input.available();
                    long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
                    long limit = 256L * available;
                    System.out.printf("payload=%d available=%d allocated=%d limit=%d%n",
                            size, available, allocated, limit);
                    // Count allocation, so the verdict does not depend on execution speed.
                    if (available <= 0 || available > size || allocated > limit) {
                        throw new AssertionError("Reading available chunked bytes allocated "
                                + allocated + " bytes for " + available + " available payload bytes; "
                                + "expected a positive count at most " + size + " and allocation at most " + limit);
                    }
                    byte[] buffer = new byte[1024];
                    int total = 0;
                    int read;
                    while ((read = input.read(buffer)) != -1) {
                        for (int i = 0; i < read; i++) {
                            if (buffer[i] != 'a') throw new AssertionError("Wrong response byte at " + (total + i));
                        }
                        total += read;
                    }
                    if (total != size) {
                        throw new AssertionError("Expected " + size + " response bytes but received " + total);
                    }
                    if (input.available() != 0 || input.read() != -1) {
                        throw new AssertionError("Expected no available bytes and EOF after the complete response");
                    }
                }
            } finally {
                connection.disconnect();
                executor.shutdownNow();
            }
        }
    }
}
