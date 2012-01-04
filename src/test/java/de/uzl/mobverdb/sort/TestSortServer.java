package de.uzl.mobverdb.sort;

import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Ordering;

import de.uzl.mobverdb.sort.base.ISortServer;

public abstract class TestSortServer {
    protected static Registry reg;
    
    /**
     * Generate a random String of given length;
     */
    protected static String generateString(Random rng, int length) {
        
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }
    
    @Test(expected=IllegalStateException.class)
    public void testNoClient() throws MalformedURLException, RemoteException, NotBoundException {
        ISortServer server = fillServerRandom(1000);
        server.sort();
    }
    
    @Test
    public void testOneClient() throws MalformedURLException, RemoteException, NotBoundException {
        ISortServer server = fillServerRandom(1000);
        server.registerClient(new SortClient());
        server.sort();
        assertTrue(Ordering.natural().isOrdered(server));
    }
    
    @Test
    public void testMultipleClients() throws MalformedURLException, RemoteException, NotBoundException {
        for(int i = 2; i<10; i++) {
            ISortServer server = fillServerRandom(1000);
            for(int j=0; j < i; j++) {
                server.registerClient(new SortClient());
            }
            server.sort();
            assertTrue(Ordering.natural().isOrdered(server));
        }
     
    }
    
    /**
     * Create a new Server fill it with a given amount of random strings (length 1-15 chars)
     */
    protected ISortServer fillServerRandom(int amount) throws MalformedURLException, RemoteException, NotBoundException {
        
        Random rand = new Random();
        ISortServer server = (ISortServer)  Naming.lookup("//127.0.0.1/server");
        for(int i = 0; i < amount; i++) {
            server.add(generateString(rand, rand.nextInt(15)+1));
        }
        return server;
    }

}
