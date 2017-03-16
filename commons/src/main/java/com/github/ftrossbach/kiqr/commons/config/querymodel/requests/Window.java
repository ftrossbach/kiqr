package com.github.ftrossbach.kiqr.commons.config.querymodel.requests;

/**
 * Created by ftr on 16/03/2017.
 */
public class Window implements Comparable<Window>{

    private long startMs;
    private long endMs;


    public Window(long startMs, long endMs) {
        this.startMs = startMs;
        this.endMs = endMs;
    }

    public Window() {
    }

    public long getStartMs() {
        return startMs;
    }

    public void setStartMs(long startMs) {
        this.startMs = startMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public void setEndMs(long endMs) {
        this.endMs = endMs;
    }


    @Override
    public int compareTo(Window o) {
        return Long.valueOf(startMs).compareTo(endMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Window window = (Window) o;

        if (startMs != window.startMs) return false;
        return endMs == window.endMs;
    }

    @Override
    public int hashCode() {
        int result = (int) (startMs ^ (startMs >>> 32));
        result = 31 * result + (int) (endMs ^ (endMs >>> 32));
        return result;
    }
}
