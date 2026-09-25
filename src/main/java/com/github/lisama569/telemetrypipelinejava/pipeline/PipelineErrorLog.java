package com.github.lisama569.telemetrypipelinejava.pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * 流水线自身异常的本地环形日志。
 * 每条记录包含阶段（stage）、可区分的原因（reason）与详情（detail），
 * 容量有限、线程安全、可本地查询。
 */
public class PipelineErrorLog {

    public record Entry(long timestampMillis, String stage, String reason, String detail) {
    }

    private final Entry[] ring;
    private final int capacity;
    private int next;
    private long totalRecorded;

    public PipelineErrorLog(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.ring = new Entry[capacity];
    }

    public synchronized void record(String stage, String reason, String detail) {
        ring[next] = new Entry(System.currentTimeMillis(), stage, reason, detail);
        next = (next + 1) % capacity;
        totalRecorded++;
    }

    /** 按时间升序返回缓冲中的记录。 */
    public synchronized List<Entry> entries() {
        List<Entry> out = new ArrayList<>();
        long size = Math.min(totalRecorded, capacity);
        for (int i = 0; i < size; i++) {
            int idx = (int) ((next - size + i + capacity) % capacity);
            if (ring[idx] != null) {
                out.add(ring[idx]);
            }
        }
        return out;
    }

    /** 历史累计记录数（含已被覆盖的）。 */
    public synchronized long totalRecorded() {
        return totalRecorded;
    }
}
