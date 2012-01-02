package de.uzl.mobverdb.sort;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestServer {

    private static Registry reg;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind("server", new Server());
    
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        
    }

    @Test
    public void test() throws MalformedURLException, RemoteException, NotBoundException {
    	
    	Server server = (Server)  Naming.lookup("//127.0.0.1/server");
    	
        String[] unsorted = new String[] {"s", "b", "a", "d","t", "c", "q"};
        
        
        server.registerClient(new Client());
        server.registerClient(new Client());
        
        for(String s : unsorted) {
        	server.add(s);
        }
        server.sort();
        
        Iterator<String> iter = server.iterator();
        
        while(iter.hasNext()) {
        	System.out.println(iter.next());
        }

    }

}
