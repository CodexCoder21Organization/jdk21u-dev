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
 * @summary A submission racing worker deactivation runs while another worker is occupied
 * @modules jdk.jdi
 * @run main/othervm OccupiedWorkerSubmission external
 * @run main/othervm OccupiedWorkerSubmission local
 */

import com.sun.jdi.Bootstrap;
import com.sun.jdi.Location;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.VMDisconnectedException;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OccupiedWorkerSubmission {
    // Breakpoints control scheduling only. The child submits through execute,
    // observes public completion/counts, and never reads or writes pool internals.
    // Source locations intentionally identify the race's three ordering points;
    // a source change that removes them requires reviewing this scheduling setup.
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        Path root = Path.of(System.getProperty("test.root", "test/jdk"));
        List<String> source = Files.readAllLines(root.resolve(
                "../../src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java"));
        int awaitStart = line(source, "private int awaitWork(WorkQueue w)", 0);
        int empty = line(source, "if (w == null)", awaitStart);
        int signalStart = line(source, "final void signalWork()", 0);
        int signalRead = line(source, "WorkQueue[] qs = queues;", signalStart);
        int deactivated = line(source, "pc, qc = ((pc - RC_UNIT) & UC_MASK) | sp)));", awaitStart) + 1;

        var connector = Bootstrap.virtualMachineManager().defaultConnector();
        var arguments = connector.defaultArguments();
        arguments.get("main").setValue(Child.class.getName() + " " + mode);
        arguments.get("options").setValue("-cp \""
                + System.getProperty("test.classes", System.getProperty("java.class.path"))
                + "\" " + System.getProperty("test.vm.opts", ""));
        arguments.get("home").setValue(System.getProperty("test.jdk", System.getProperty("java.home")));
        VirtualMachine vm = connector.launch(arguments);
        Process child = vm.process();
        Thread stdout = Thread.ofPlatform().daemon().start(() -> copy(child.getInputStream(), System.out));
        Thread stderr = Thread.ofPlatform().daemon().start(() -> copy(child.getErrorStream(), System.err));
        PrintWriter commands = new PrintWriter(child.getOutputStream(), true);
        EventSet worker = null, producer = null;
        int stage = 0;
        try {
            var prepare = vm.eventRequestManager().createClassPrepareRequest();
            prepare.addClassFilter("java.util.concurrent.ForkJoinPool");
            prepare.setSuspendPolicy(EventRequest.SUSPEND_ALL);
            prepare.enable();
            vm.resume();
            boolean finished = false;
            while (!finished) {
                EventSet events = vm.eventQueue().remove(10_000);
                if (events == null)
                    throw new AssertionError("Child did not reach ordering stage " + stage);
                boolean hold = false;
                for (Event event : events) {
                    if (event instanceof ClassPrepareEvent e) {
                        breakpoint(vm, e.referenceType(), empty, "empty");
                        breakpoint(vm, e.referenceType(), signalRead, "producer");
                        breakpoint(vm, e.referenceType(), deactivated, "inactive");
                        prepare.disable();
                    } else if (event instanceof BreakpointEvent e) {
                        String point = (String)e.request().getProperty("point");
                        String thread = e.thread().name();
                        if (point.equals("empty") && thread.equals("available-worker") && stage == 0) {
                            e.request().disable();
                            worker = events;
                            hold = true;
                            stage = 1;
                            commands.println("SUBMIT");
                        } else if (point.equals("producer")
                                && thread.equals(mode.equals("local") ? "occupied-worker" : "external-producer")
                                && stage == 1) {
                            e.request().disable();
                            producer = events;
                            hold = true;
                            stage = 2;
                            worker.resume();
                            worker = null;
                        } else if (point.equals("inactive") && thread.equals("available-worker") && stage == 2) {
                            e.request().disable();
                            stage = 3;
                            events.resume();
                            producer.resume();
                            producer = null;
                            hold = true; // already resumed, exactly once
                        }
                    } else if (event instanceof VMDeathEvent) {
                        finished = true;
                    }
                }
                if (!hold) events.resume();
            }
        } catch (VMDisconnectedException disconnected) {
            // Process status below is authoritative once the child disconnects.
        } finally {
            try { vm.dispose(); } catch (VMDisconnectedException disconnected) {
                // A normally exited child has already disconnected.
            }
            if (!child.waitFor(10, TimeUnit.SECONDS)) {
                child.destroyForcibly();
                child.waitFor();
                throw new AssertionError("Child did not exit after debugger detached at stage " + stage);
            }
            stdout.join();
            stderr.join();
            commands.close();
        }
        if (stage != 3 || child.exitValue() != 0)
            throw new AssertionError("Submission failed: mode=" + mode
                    + ", stage=" + stage + ", exit=" + child.exitValue());
        System.out.println("PASS " + mode + ": empty scan, producer read, deactivation, completion");
    }

    static int line(List<String> source, String text, int after) {
        for (int i = after; i < source.size(); i++)
            if (source.get(i).strip().startsWith(text)) return i + 1;
        throw new AssertionError("Ordering point missing from source: " + text);
    }

    static void breakpoint(VirtualMachine vm, ReferenceType type, int line, String point) throws Exception {
        List<Location> locations = type.locationsOfLine(line);
        if (locations.isEmpty()) throw new AssertionError("No debugger location for " + point + " at " + line);
        BreakpointRequest request = vm.eventRequestManager().createBreakpointRequest(locations.getFirst());
        request.putProperty("point", point);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        request.enable();
    }

    static void copy(java.io.InputStream in, java.io.OutputStream out) {
        try { in.transferTo(out); } catch (java.io.IOException failure) {
            throw new AssertionError("Could not read child output", failure);
        }
    }

    static void await(CountDownLatch latch, String description) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("Did not reach " + description);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted at " + description, failure);
        }
    }

    public static class Child {
        public static void main(String[] args) throws Exception {
            boolean local = args[0].equals("local");
            AtomicInteger ids = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch occupied = new CountDownLatch(1), release = new CountDownLatch(1);
            CountDownLatch submit = new CountDownLatch(1), completed = new CountDownLatch(1);
            ForkJoinPool pool = new ForkJoinPool(2, p -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                t.setName(ids.incrementAndGet() == 1 ? "occupied-worker" : "available-worker");
                return t;
            }, null, true, 0, 2, 1, p -> true, 30, TimeUnit.SECONDS);
            Thread external = null;
            try {
                pool.execute(() -> {
                    occupied.countDown();
                    try {
                        if (local) {
                            await(submit, "local submission release");
                            pool.execute(completed::countDown);
                        }
                        // Release only from finally: a timed blocker could rescue
                        // the queued task at the very bound that detects the bug.
                        release.await();
                    } catch (Throwable t) { failure.set(t); }
                });
                await(occupied, "occupied worker");
                pool.execute(() -> { });
                String command = new BufferedReader(new InputStreamReader(System.in)).readLine();
                if (!"SUBMIT".equals(command)) throw new AssertionError("Expected SUBMIT, got " + command);
                if (pool.getActiveThreadCount() != 2)
                    throw new AssertionError("Both workers must be active before producer reads control count: " + pool);
                if (local) submit.countDown();
                else external = Thread.ofPlatform().name("external-producer").start(() -> {
                    try { pool.execute(completed::countDown); } catch (Throwable t) { failure.set(t); }
                });
                if (!completed.await(10, TimeUnit.SECONDS)) {
                    System.err.println("Queued submission: " + pool);
                    for (var entry : Thread.getAllStackTraces().entrySet()) {
                        System.err.println(entry.getKey());
                        for (var frame : entry.getValue()) System.err.println("    at " + frame);
                    }
                    throw new AssertionError("Submission did not execute while one worker was available");
                }
                if (failure.get() != null) throw new AssertionError("Worker/producer failed", failure.get());
            } finally {
                submit.countDown();
                release.countDown();
                if (external != null) external.join(5_000);
                pool.shutdown();
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) throw new AssertionError("Pool did not terminate");
            }
        }
    }
}
