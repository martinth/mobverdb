package de.uzl.mobverdb.join;

import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.collect.Iterables;

import de.uzl.mobverdb.join.data.Row;

public class JoinUtils {
    
    public static Row[] nestedLoopJoin(Iterable<Row> dataA, Iterable<Row> dataB) {
        ArrayList<Row> output = new ArrayList<Row>();
        
        for (Row lineA : dataA) {
            for(Row lineB : dataB) {
                if(lineA.getKey().equals(lineB.getKey())) {
                    output.add( new Row(lineA.getKey(), Iterables.concat(lineA.getData(), lineB.getData())) );
                }
            }
        }
        
        return output.toArray(new Row[] {});
    }
    
    public static Row[] nestedLoopJoin(Row[] dataA, Row[] dataB) {
        return JoinUtils.nestedLoopJoin(Arrays.asList(dataA), Arrays.asList(dataB));
    }
    

}
