package de.uzl.mobverdb.sort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
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
    private int blockSize;

    public SortServer(int numClients, boolean useDistSort, String inputData, int blockSize) throws IOException {
        this.server = useDistSort ? new DistributionSort(blockSize) : new MergeSort(blockSize);
        this.inputData = inputData;
        this.numClients = numClients;
        this.blockSize = blockSize;
        
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        reg.rebind(SERVER_NAME, server);
        
        String mode = useDistSort ? "distribution sort" : "merge sort";
        log.info("Started server with mode: "+ mode);
        
    }
    
    public void run() {
        
        
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
                Thread.sleep(1000);
                int currentCount = server.getClientCount();
                
                if(currentCount != lastCount) {
                    log.debug(String.format("%d clients connected. Waiting", currentCount));
                    lastCount = currentCount;
                }
                if(currentCount >= numClients) {
                    server.sort();
                    log.debug("Sorting finished");
                    
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
        
        try {
            FileWriter fstream = new FileWriter(PERF_FILE);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write("blocksize, clients, add (msec), distribute (msec), fetch/iter (msec)\n");
            out.write(String.format("%s, %s, %s, %s, %s\n",
                this.blockSize, this.numClients,
                addWatch.elapsedMillis(), server.distWatch.elapsedMillis(), iterateWatch.elapsedMillis()));
            out.close();
        } catch (IOException e) {
            log.error("Could not write performance file", e);
        }
       
       
        System.exit(0);
    }
}