package de.uzl.mobverdb.sort;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Ordering;

public class TestMergeSortServer {
    
    private static Registry reg;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind("server", new MergeSortServer());
    }
    
    /**
     * Generate a random String of given length;
     */
    public static String generateString(Random rng, int length) {
        
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    @Test(expected=IllegalStateException.class)
    public void testNoClient() throws MalformedURLException, RemoteException, NotBoundException {
        MergeSortServer server = fillServerRandom(1000);
        server.sort();
    }
    
    @Test
    public void testOneClient() throws MalformedURLException, RemoteException, NotBoundException {
        MergeSortServer server = fillServerRandom(1000);
        server.registerClient(new MergeSortClient());
        server.sort();
        assertTrue(Ordering.natural().isOrdered(server));
    }
    
    @Test
    public void testMultipleClients() throws MalformedURLException, RemoteException, NotBoundException {
        for(int i = 2; i<10; i++) {
            MergeSortServer server = fillServerRandom(1000);
            for(int j=0; j < i; j++) {
                server.registerClient(new MergeSortClient());
            }
            server.sort();
            assertTrue(Ordering.natural().isOrdered(server));
        }
     
    }
    
    /**
     * Create a new Server fill it with a given amount of random strings (length 1-15 chars)
     */
    private MergeSortServer fillServerRandom(int amount) throws MalformedURLException, RemoteException, NotBoundException {
        
        Random rand = new Random();
        MergeSortServer server = (MergeSortServer)  Naming.lookup("//127.0.0.1/server");
        for(int i = 0; i < amount; i++) {
            server.add(generateString(rand, rand.nextInt(15)+1));
        }
        return server;
    }


}
