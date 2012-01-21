package de.uzl.mobverdb.join.modes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Iterables;


import de.uzl.mobverdb.join.data.CSVData;
import de.uzl.mobverdb.join.data.Row;


/**
 * Implements a local nested-loop join.
 * @author Martin Thurau
 *
 */
public class LocalJoin extends AbstractJoin {
    
    private CSVData dataA;
    private CSVData dataB;
    
    public LocalJoin(File fileA, File fileB) throws NumberFormatException, IOException {
        this.dataA = new CSVData(fileA);
        this.dataB = new CSVData(fileB);
    }

    @Override
    protected Row[] doJoin() {
        
        ArrayList<Row> output = new ArrayList<Row>();
        
        for (Row lineA : this.dataA.lines) {
            for(Row lineB : this.dataB.lines) {
                if(lineA.getKey().equals(lineB.getKey())) {
                    output.add( new Row(lineA.getKey(), Iterables.concat(lineA.getData(), lineB.getData())) );
                }
            }
        }
        
        return output.toArray(new Row[] {});
    }

}
