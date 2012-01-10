package de.uzl.mobverdb.sort;

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

    
    private BaseSort server;
    private String inputData;
    private int numClients;
    
    private Stopwatch addWatch = new Stopwatch();
    private Stopwatch iterateWatch = new Stopwatch();

    public SortServer(int numClients, boolean useDistSort, String inputData) throws IOException {
        this.server = useDistSort ? new DistributionSort() : new MergeSort();
        this.inputData = inputData;
        this.numClients = numClients;
        
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        reg.rebind(SERVER_NAME, server);
        
        String mode = useDistSort ? "distribution sort" : "merge sort";
        log.info("Started server with mode: "+ mode);
        
    }
    
    public void run() {
        
        log.debug("Reading inputfile");
        
        addWatch.start();
        for (String string : Splitter.on(CharMatcher.INVISIBLE).split(inputData)) {
            server.add(string);
        }
        addWatch.stop();
        log.info("Input file completely read");
        
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
                    log.info("Starting sort");
                    server.sort();
                    log.info("Sorting finished");
                    
                    log.info("Starting to get results");
                    iterateWatch.start();
                    Iterator<String> iter = server.iterator();
                    while(iter.hasNext()) {
                        iter.next();
                    }
                    iterateWatch.stop();
                    log.info("All results fetched");
                    
                    isFinished = true;
                }
                
                
            } catch (RemoteException e) {
                log.fatal("Error while sorting:", e);
                System.exit(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info(String.format("Perf: %s; %s; %s", 
            addWatch.elapsedMillis(), server.distWatch.elapsedMillis(), iterateWatch.elapsedMillis()));
       
        System.exit(0);
    }
}