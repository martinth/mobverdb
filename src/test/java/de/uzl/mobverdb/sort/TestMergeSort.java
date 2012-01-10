package de.uzl.mobverdb.sort;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.Before;
import org.junit.BeforeClass;

import de.uzl.mobverdb.sort.remote.MergeSort;

public class TestMergeSort extends TestSortServer {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if(reg == null) reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
    }

    @Override
    @Before
    public void rebind() throws Exception {
        Naming.rebind("server", new MergeSort());
    }
}
