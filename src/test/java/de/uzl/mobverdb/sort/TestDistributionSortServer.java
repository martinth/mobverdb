package de.uzl.mobverdb.sort;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import org.junit.BeforeClass;

public class TestDistributionSortServer extends TestSortServer {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind("server", new DistributionSortServer());
    }
}
