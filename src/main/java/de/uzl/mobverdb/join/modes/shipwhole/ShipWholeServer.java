package de.uzl.mobverdb.join.modes.shipwhole;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;

import de.uzl.mobverdb.join.data.Row;
import de.uzl.utils.Threads;

public class ShipWholeServer extends UnicastRemoteObject implements IShipWholeServer {
    
    private static final long serialVersionUID = -5430396965086442250L;
    private final Logger log = Logger.getLogger(this.getClass().getCanonicalName());
    private ArrayList<IShipWholeClient> clients = new ArrayList<IShipWholeClient>();
    ArrayList<Row> output = new ArrayList<Row>();
    public final static String BIND_NAME = "shipWholeServer"; 
    
    public ShipWholeServer() throws RemoteException {
        super();
    }

    public void serve() throws RemoteException, MalformedURLException {
        Registry reg = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        Naming.rebind(BIND_NAME, this);
        
        log.info("Server bound, waiting for clients");
        
        while(clients.size() < 2) {
            Threads.trySleep(1000);
        }
        log.info("Two clients connect, fetching their data");
        
        Row[] dataR = this.clients.get(0).getData();
        Row[] dataS = this.clients.get(1).getData();
        
        log.info("Data fetched, beginning to join");
        
        
        for (Row lineR : dataR) {
            for(Row lineS : dataS) {
                if(lineR.getKey().equals(lineS.getKey())) {
                    output.add( new Row(lineR.getKey(), Iterables.concat(lineR.getData(), lineS.getData())) );
                }
            }
        }
        log.info("Join completed");
        
        this.clients.clear();
        try {
            Naming.unbind(BIND_NAME);
        } catch (NotBoundException e) {
            // we ignore this
        }
        UnicastRemoteObject.unexportObject(reg, true);
        System.exit(0);
    }
    
    public List<Row> getResults() {
        return this.output;
    }

    @Override
    public void register(IShipWholeClient client) throws RemoteException {
        this.clients.add(client);
    }
    
}