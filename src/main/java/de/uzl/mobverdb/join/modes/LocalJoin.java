package de.uzl.mobverdb.join.modes;

import java.io.File;
import java.io.IOException;

import de.uzl.mobverdb.join.JoinUtils;
import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;


/**
 * Implements a local nested-loop join.
 * @author Martin Thurau
 *
 */
public class LocalJoin implements MeasurableJoin {
    
    private CSVData dataA;
    private CSVData dataB;
    private JoinPerf joinPerf = new JoinPerf();
    
    public LocalJoin(File fileA, File fileB) throws NumberFormatException, IOException {
        this.dataA = new CSVData(fileA);
        this.dataB = new CSVData(fileB);
    }

    public Row[] join() {
        joinPerf.startAll();
        Row[] result = JoinUtils.nestedLoopJoin(this.dataA.lines, this.dataB.lines);
        joinPerf.stopAll();
        return result;
    }

    @Override
    public JoinPerf getPerf() {
        return this.joinPerf;
    }

}
