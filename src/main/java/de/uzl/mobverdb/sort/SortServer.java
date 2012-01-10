package de.uzl.mobverdb.sort;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import de.uzl.mobverdb.sort.remote.BaseSort;
import de.uzl.mobverdb.sort.remote.DistributionSort;
import de.uzl.mobverdb.sort.remote.MergeSort;

public class SortServer extends Thread {
    
    private final static Logger log = Logger.getLogger(Sorting.class.getCanonicalName());
    public final static String SERVER_NAME = "sortServer";

    
    private BaseSort server;
    private String inputData;
    private int numClients;

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
        
        for (String string : Splitter.on(CharMatcher.INVISIBLE).split(inputData)) {
            server.add(string);
        }
        log.info("Input file completely read");
        
        boolean isFinished = false;
        while(!isFinished) {
            try {
                Thread.sleep(1000);
                log.debug(String.format("%d clients connected. Waiting", server.getClientCount()));
                if(server.getClientCount() >= numClients) {
                    log.info(String.format("%d clients connected. Strating sort", server.getClientCount()));
                    server.sort();
                    log.info("Sorting finished");
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
}