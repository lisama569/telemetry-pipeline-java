package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

import java.util.concurrent.Executor;

/**
 * Executor decorator that captures the submitting thread's {@link TraceContext}
 * and binds a child of it around the task's execution, so asynchronous work
 * stays correlated with the originating request. Tasks submitted without an
 * active context run unchanged (no fabricated context).
 */
public final class ContextPropagatingExecutor implements Executor {

    private final Executor delegate;

    public ContextPropagatingExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable command) {
        TraceContext captured = TraceContextHolder.current();
        if (captured == null) {
            delegate.execute(command);
            return;
        }
        TraceContext child = captured.child();
        delegate.execute(() -> {
            try (TraceContextHolder.Scope ignored = TraceContextHolder.open(child)) {
                command.run();
            } finally {
                TraceContextHolder.clear();
            }
        });
    }
}
