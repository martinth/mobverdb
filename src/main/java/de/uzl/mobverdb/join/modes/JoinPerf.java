package de.uzl.mobverdb.join.modes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;


public class JoinPerf {
    
    public final Stopwatch prepareTime = new Stopwatch();
    public final Stopwatch localJoinTime = new Stopwatch();
    public final Stopwatch remoteJoinTime = new Stopwatch();
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
        this.prepareTime.start();
        this.localJoinTime.start();
        this.remoteJoinTime.start();
    }

    public void stopAll() {
        this.totalTime.stop();
        this.prepareTime.stop();
        this.localJoinTime.stop();
        this.remoteJoinTime.stop();
    }
    
    public String toString() {
        return Objects.toStringHelper(this)
            .add("prepareTime", prepareTime.elapsedMillis())
            .add("localJoin", localJoinTime.elapsedMillis())
            .add("remoteJoin", remoteJoinTime.elapsedMillis())
            .add("totalTime", totalTime.elapsedMillis())
            .add("rmiCalls", rmiCalls)
            .toString();
    }
    
    public void writeTofile(String filename) throws IOException {
        
        FileWriter fstream = new FileWriter(filename);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("jointype, prepare (msec) , localjoin (msec), remotejoin (msec), total (msec), rmi calls (#)\n");
        out.write(
            String.format("%s, %s, %s, %s, %s, %s\n",
                joinType,
                prepareTime.elapsedMillis(),
                localJoinTime.elapsedMillis(),
                remoteJoinTime.elapsedMillis(),
                totalTime.elapsedMillis(),
                rmiCalls
            )
        );
        out.close();
        
    }
}
