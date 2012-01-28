package de.uzl.mobverdb.join.modes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;


public class JoinPerf {
    
    public final Stopwatch joinTime = new Stopwatch();
    public final Stopwatch totalTime = new Stopwatch();
    private int rmiCalls = 0;
    private String joinType;
    
    public JoinPerf(String joinType) {
        this.joinType = joinType;
    }

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
    
    public void writeTofile(String filename) throws IOException {
        
        FileWriter fstream = new FileWriter(filename);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("jointype, join (msec), total (msec), rmi calls (#)\n");
        out.write(
            String.format("%s, %s, %s, %s\n",
                joinType, joinTime.elapsedMillis(), totalTime.elapsedMillis(),
                rmiCalls
            )
        );
        out.close();
        
    }
}
