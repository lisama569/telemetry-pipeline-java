package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

/**
 * Thread-local holder for the current request's {@link TraceContext}.
 * {@link #open(TraceContext)} returns a {@link Scope} that restores the
 * previous context on close, so nested and async calls never leak or
 * cross-wire contexts between requests.
 */
public final class TraceContextHolder {

    private static final ThreadLocal<TraceContext> CURRENT = new ThreadLocal<>();

    private TraceContextHolder() {
    }

    /** A handle that restores the previous context when closed. */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    /** The context bound to the current thread, or {@code null} if none. */
    public static TraceContext current() {
        return CURRENT.get();
    }

    /**
     * Binds {@code context} to the current thread until the returned scope is
     * closed. Closing restores whatever was bound before (possibly null).
     */
    public static Scope open(TraceContext context) {
        TraceContext previous = CURRENT.get();
        CURRENT.set(context);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    /** Removes any context bound to the current thread. */
    public static void clear() {
        CURRENT.remove();
    }
}
