package com.github.lisama569.telemetrypipelinejava.tracing;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * 基于 ThreadLocal 的追踪上下文持有者。
 * 异步/嵌套调用通过 wrap(...) 显式捕获并恢复上下文，
 * 保证任务执行期间上下文关联、执行后工作线程不残留（防串线）。
 */
public final class TraceContextHolder {

    private static final ThreadLocal<TraceContext> CURRENT = new ThreadLocal<>();

    private TraceContextHolder() {
    }

    public static TraceContext current() {
        return CURRENT.get();
    }

    public static void set(TraceContext context) {
        if (context == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(context);
        }
    }

    public static void clear() {
        CURRENT.remove();
    }

    /** 捕获当前上下文，包装 Runnable 以便在其它线程执行时恢复。 */
    public static Runnable wrap(Runnable task) {
        TraceContext captured = CURRENT.get();
        return () -> {
            TraceContext previous = CURRENT.get();
            if (captured != null) {
                CURRENT.set(captured);
            }
            try {
                task.run();
            } finally {
                if (previous != null) {
                    CURRENT.set(previous);
                } else {
                    CURRENT.remove();
                }
            }
        };
    }

    /** 捕获当前上下文，包装 Supplier 以便在其它线程执行时恢复。 */
    public static <T> Supplier<T> wrapSupplier(Supplier<T> supplier) {
        TraceContext captured = CURRENT.get();
        return () -> {
            TraceContext previous = CURRENT.get();
            if (captured != null) {
                CURRENT.set(captured);
            }
            try {
                return supplier.get();
            } finally {
                if (previous != null) {
                    CURRENT.set(previous);
                } else {
                    CURRENT.remove();
                }
            }
        };
    }

    /** 捕获当前上下文，包装 Callable 以便在其它线程执行时恢复。 */
    public static <T> Callable<T> wrap(Callable<T> task) {
        TraceContext captured = CURRENT.get();
        return () -> {
            TraceContext previous = CURRENT.get();
            if (captured != null) {
                CURRENT.set(captured);
            }
            try {
                return task.call();
            } finally {
                if (previous != null) {
                    CURRENT.set(previous);
                } else {
                    CURRENT.remove();
                }
            }
        };
    }
}
