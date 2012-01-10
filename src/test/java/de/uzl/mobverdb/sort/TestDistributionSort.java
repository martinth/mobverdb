package de.uzl.mobverdb.sort;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.Before;
import org.junit.BeforeClass;

import de.uzl.mobverdb.sort.remote.DistributionSort;

public class TestDistributionSort extends TestSortServer {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if(reg == null) reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
    }
    

    @Before
    public
    void rebind() throws Exception {
        Naming.rebind("server", new DistributionSort());
    }
}
