package de.uzl.mobverdb.join.modes;

import com.google.common.base.Stopwatch;

import de.uzl.mobverdb.join.data.Row;

public abstract class AbstractJoin {
    
    Stopwatch jointime = new Stopwatch();

    /**
     * Do the join
     * @return the joined data
     */
    public Row[] join() {
               
        jointime.start();
        Row[] result = this.doJoin();
        jointime.stop();
        
        return result;
    }

    
    /**
     * Should implement the join
     * @return the joined data
     */
    protected abstract Row[] doJoin();

}
