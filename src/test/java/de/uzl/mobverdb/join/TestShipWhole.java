package de.uzl.mobverdb.join;

import java.io.File;
import java.rmi.RemoteException;

import org.junit.Test;
import org.junit.Assert;

import de.uzl.mobverdb.AsyncTester;
import de.uzl.mobverdb.join.modes.shipwhole.ShipWholeClient;
import de.uzl.mobverdb.join.modes.shipwhole.ShipWholeServer;

public class TestShipWhole{
    
    
    public void testShipWhole() throws Exception {
        AsyncTester server = new AsyncTester(new Runnable() {
            public void run() {
                try {
                    ShipWholeServer s = new ShipWholeServer();
                    s.serve();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }
        });
        server.start();
        
        Thread.sleep(1000);
        
        AsyncTester c1 = new AsyncTester(new Runnable() {
            public void run() {
                try {
                    ShipWholeClient client = new ShipWholeClient(new File("join-data-a.csv"), "127.0.0.1");
                } catch (Exception e) {
                    Assert.fail(e.toString());
                }
            }
        });
        c1.start();
        
        AsyncTester c2 = new AsyncTester(new Runnable() {
            public void run() {
                try {
                    ShipWholeClient client = new ShipWholeClient(new File("join-data-a.csv"), "127.0.0.1");
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }
        });
        c2.start();
        
        c1.test();
        c2.test();
               
        
    }

}
