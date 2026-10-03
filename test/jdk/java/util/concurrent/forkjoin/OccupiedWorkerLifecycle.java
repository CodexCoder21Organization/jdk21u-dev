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
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/*
 * @test
 * @summary Worker deactivation preserves shutdown, factory-failure and compensation behavior
 * @run main OccupiedWorkerLifecycle shutdown
 * @run main OccupiedWorkerLifecycle stop
 * @run main OccupiedWorkerLifecycle factory
 * @run main OccupiedWorkerLifecycle compensation
 */

import java.util.ArrayList;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OccupiedWorkerLifecycle {
    static void await(CountDownLatch latch, String description) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("Did not reach " + description);
    }

    static void terminate(ForkJoinPool pool) throws InterruptedException {
        pool.shutdownNow();
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("Pool did not terminate: " + pool);
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "shutdown" -> shutdown();
            case "stop" -> stop();
            case "factory" -> factory();
            case "compensation" -> compensation();
            default -> throw new IllegalArgumentException("Unknown scenario: " + args[0]);
        }
        System.out.println("PASS lifecycle " + args[0]);
    }

    static void shutdown() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(2);
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1_000);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int i = 0; i < 2; i++) pool.execute(() -> {
                entered.countDown();
                try { release.await(); } catch (InterruptedException t) { failure.set(t); }
            });
            await(entered, "both occupied workers");
            for (int i = 0; i < 1_000; i++) pool.execute(completed::countDown);
            pool.shutdown();
            try {
                pool.execute(() -> { throw new AssertionError("Post-shutdown task executed"); });
                throw new AssertionError("Post-shutdown submission accepted");
            } catch (RejectedExecutionException expected) { }
            release.countDown();
            await(completed, "all accepted tasks after shutdown");
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("Orderly shutdown did not terminate");
            if (!pool.isTerminated() || failure.get() != null) throw new AssertionError("Shutdown failed", failure.get());
        } finally {
            release.countDown();
            terminate(pool);
        }
    }

    static void stop() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(2);
        CountDownLatch entered = new CountDownLatch(2), interrupted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            for (int i = 0; i < 2; i++) pool.execute(() -> {
                entered.countDown();
                try { release.await(); } catch (InterruptedException expected) { interrupted.countDown(); }
            });
            await(entered, "workers blocked before shutdownNow");
            var tasks = new ArrayList<ForkJoinTask<?>>();
            for (int i = 0; i < 1_000; i++) tasks.add(pool.submit(() -> { }));
            pool.shutdownNow();
            await(interrupted, "both shutdownNow interruptions");
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("Immediate shutdown did not terminate");
            // shutdownNow attempts to cancel queued tasks; an already-starting
            // task may complete. Every returned task must reach a terminal state.
            for (var task : tasks)
                if (!task.isDone()) throw new AssertionError("Task retained after immediate shutdown: " + task);
        } finally {
            release.countDown();
            terminate(pool);
        }
    }

    static void factory() throws Exception {
        IllegalStateException expected = new IllegalStateException("Worker construction rejected by test factory");
        AtomicInteger attempts = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(1, p -> {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) throw expected;
            if (attempt == 2) return null;
            return ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
        }, null, true);
        try {
            try {
                pool.execute(() -> { });
                throw new AssertionError("Factory failure did not reach submitter");
            } catch (IllegalStateException observed) {
                if (observed != expected) throw new AssertionError("Original factory failure was replaced", observed);
            }
            if (pool.getPoolSize() != 0) throw new AssertionError("Failed worker reservation retained: " + pool);
            pool.execute(() -> { });
            if (pool.getPoolSize() != 0) throw new AssertionError("Null worker reservation retained: " + pool);
            CountDownLatch recovered = new CountDownLatch(1);
            pool.execute(recovered::countDown);
            await(recovered, "valid worker after two factory failures");
            if (attempts.get() != 3) throw new AssertionError("Unexpected factory call count: " + attempts);
        } finally { terminate(pool); }
    }

    static void compensation() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(1);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean blocking = new AtomicBoolean();
        try {
            pool.execute(() -> {
                try {
                    ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
                        public boolean isReleasable() { return release.getCount() == 0; }
                        public boolean block() throws InterruptedException {
                            blocking.set(true);
                            entered.countDown();
                            release.await();
                            return true;
                        }
                    });
                } catch (Throwable t) { failure.set(t); }
            });
            await(entered, "managed blocking");
            pool.execute(completed::countDown);
            await(completed, "compensating worker task");
            if (!blocking.get()) throw new AssertionError("ManagedBlock did not block");
            release.countDown();
            pool.shutdown();
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("Compensating pool did not terminate");
            if (failure.get() != null) throw new AssertionError("ManagedBlock failed", failure.get());
        } finally {
            release.countDown();
            terminate(pool);
        }
    }
}
