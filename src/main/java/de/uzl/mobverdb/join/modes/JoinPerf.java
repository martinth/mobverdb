package de.uzl.mobverdb.join.modes;

import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;


public class JoinPerf {
    
    public final Stopwatch joinTime = new Stopwatch();
    public final Stopwatch totalTime = new Stopwatch();
    private int rmiCalls = 0;

    public void rmiCall() {
        this.rmiCalls++;
    }
    
    public void rmiCalls(int i) {
        this.rmiCalls += i;
    }
    
    public void startAll() {
        this.totalTime.start();
        this.joinTime.start();
    }

    public void stopAll() {
        this.totalTime.stop();
        this.joinTime.stop();
    }
    
    public String toString() {
        return Objects.toStringHelper(this)
            .add("joinTime", joinTime.elapsedMillis())
            .add("totalTime", totalTime.elapsedMillis())
            .add("rmiCalls", rmiCalls)
            .toString();
    }
}
