package de.uzl.mobverdb.join.data;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

/**
 * Load sample data from CSV file. The lines attribute will contain all lines of the CSV file
 * wrapped into individual Line objects. 
 * 
 * @author Martin Thurau
 */
public class CSVData {
    
    public final Row[] lines;
    
    /**
     * Creates a new CSVData instance, which contents are loaded from a CSV file. The first column
     * has to be a valid integer.
     * @param inputFile which file to load
     * @throws NumberFormatException if the first column doesn't contain a valid integer
     * @throws IOException
     */
    public CSVData(File inputFile) throws NumberFormatException, IOException {
        this.lines = Files.readLines(inputFile, Charset.defaultCharset(), new LineProcessor<Row[]>() {
            private ArrayList<Row> tmpLines = new ArrayList<Row>();
            
            public boolean processLine(String line) throws IOException {
                Iterable<String> fields = Splitter.on(',').trimResults().split(line);
                tmpLines.add( new Row(fields) );
                return true; // continue reading
            }

            public Row[] getResult() {
                return tmpLines.toArray(new Row[] {});
            }
        });
    }

}
