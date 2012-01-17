package de.uzl.mobverdb.sort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

import de.uzl.mobverdb.sort.Sorting.Sorttype;
import de.uzl.mobverdb.sort.remote.BaseSort;
import de.uzl.mobverdb.sort.remote.DistributionSort;
import de.uzl.mobverdb.sort.remote.MergeSort;

public class SortServer extends Thread {
    
    private final static Logger log = Logger.getLogger(Sorting.class.getCanonicalName());
    public final static String SERVER_NAME = "sortServer";
    private final static String PERF_FILE = "perf.log";

    
    private BaseSort server;
    private String inputData;
    private int numClients;
    
    private Stopwatch addWatch = new Stopwatch();
    private Stopwatch iterateWatch = new Stopwatch();
    private Stopwatch totalWatch = new Stopwatch();
    private int blockSize;
    private Sorttype sortType;

    public SortServer(int numClients, Sorttype sortType, String inputData, int blockSize) throws IOException {
        this.sortType = sortType;
        this.inputData = inputData;
        this.numClients = numClients;
        this.blockSize = blockSize;
        
        if(sortType == Sorttype.LOCAL) {
            this.server = null;
        } else {
            if(sortType == Sorttype.MERGE) {
                this.server = new MergeSort(blockSize);
            } else {
                this.server = new DistributionSort(blockSize);
            }
            Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
            reg.rebind(SERVER_NAME, server);         
        }
        log.info("Started server with mode: "+ sortType);
    }
    
    public void run() {
        
        totalWatch.start();
        
        if(this.server == null) { // do local sort
            localSort();
        } else {
            // split data
            addWatch.start();
            for (String string : Splitter.on(CharMatcher.INVISIBLE).split(inputData)) {
                server.add(string);
            }
            addWatch.stop();
            log.debug("Input file completely read");
            
            boolean isFinished = false;
            int lastCount = -1;
            while(!isFinished) {
                try {
                    Thread.sleep(100);
                    int currentCount = server.getClientCount();
                    // wait for enough clients to connect
                    if(currentCount != lastCount) {
                        log.debug(String.format("%d clients connected. Waiting", currentCount));
                        lastCount = currentCount;
                    }
                    if(currentCount >= numClients) {
                        // enough clients - do sorting
                        server.sort();
                        log.debug("Sorting finished");
                        // iterate over results
                        iterateWatch.start();
                        Iterator<String> iter = server.iterator();
                        while(iter.hasNext()) {
                            iter.next();
                        }
                        iterateWatch.stop();
                        log.debug("All results fetched");
                        isFinished = true;
                    }
                } catch (RemoteException e) {
                    log.fatal("Error while sorting:", e);
                    System.exit(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        totalWatch.stop();
        
        try {
            FileWriter fstream = new FileWriter(PERF_FILE);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write("sorttype, blocksize, clients, add (msec), distribute (msec), fetch/iter (msec), total (msec)\n");
            out.write(
                String.format("%s, %s, %s, %s, %s, %s, %s\n",
                    sortType, blockSize, numClients,
                    addWatch.elapsedMillis(), 
                    server == null ? 0 : server.distWatch.elapsedMillis(), 
                    iterateWatch.elapsedMillis(),
                    totalWatch.elapsedMillis()
                )
            );
            out.close();
        } catch (IOException e) {
            log.error("Could not write performance file", e);
        }
       
       
        System.exit(0);
    }
    
    private void localSort() {
        ArrayList<String> toSort = new ArrayList<String>();
        addWatch.start();
        for (String string : Splitter.on(CharMatcher.INVISIBLE).split(inputData)) {
            toSort.add(string);
        }
        addWatch.stop();
        
        Collections.sort(toSort);
        
    }
}