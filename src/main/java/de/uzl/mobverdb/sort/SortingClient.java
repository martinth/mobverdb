package de.uzl.mobverdb.sort;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import de.uzl.mobverdb.sort.remote.Client;
import de.uzl.mobverdb.sort.remote.interfaces.ISortServer;

public class SortingClient extends Thread {
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    
 private ISortServer server;

private Client client;
    
    
    public SortingClient(String url) throws MalformedURLException, RemoteException, NotBoundException {
        this.client = new Client();
        this.server = (ISortServer) Naming.lookup(url);
    }
    

    
    public void run() {
        
        
        
        try {
            
            server.registerClient(client);
            log.info("Client registered at server");
            
            while(!client.isFinished()) {
                Thread.sleep(1000);
            }
            log.debug("Client is finished. Waiting 5 secs...");
            Thread.sleep(5000);
            log.info("Client shutting down");
            System.exit(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    } 
}
