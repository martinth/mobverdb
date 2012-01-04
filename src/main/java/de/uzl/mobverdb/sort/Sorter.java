package de.uzl.mobverdb.sort;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import de.uzl.mobverdb.sort.base.ISortServer;

public class Sorter {
    
    private final static Logger log = Logger.getLogger(Sorter.class.getCanonicalName());
    private final static String SERVER_NAME = "sortServer";
    private SortClient client;
    private ISortServer server;
    
    public Sorter(Remote remote) {
        server = (ISortServer) remote;
        client = new SortClient();
        
    }
    
    public void runAndWait() {
        try {
            server.registerClient(client);
            try {
                while(!client.isFinished()) {
                    Thread.sleep(2000);
                }
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
        } catch (RemoteException e) {
            log.fatal("Could not register at server", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("s", "server", true, "IP/hostname of the server (optionally with :port)");
        CommandLineParser parser = new PosixParser();
        
        try {
            setupAndrun(parser.parse( options, args));
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(e.getMessage());
            formatter.printHelp("", options);
        }
    }
    
    /**
     * Do the setup and run the app
     * @param cmd the parsed CommandLine
     * @throws Exception on errors
     */
    private static void setupAndrun(CommandLine cmd) throws ParseException {
        
            try {
                Remote remote = Naming.lookup("//"+cmd.getOptionValue("s")+"/"+SERVER_NAME);
                Sorter sorter = new Sorter(remote);
                sorter.runAndWait();
            } catch (MalformedURLException e) {
                throw new ParseException(e.getMessage());
            } catch (RemoteException e) {
                log.fatal("Registry at "+cmd.getOptionValue("s")+" could not be contacted.", e);
                System.exit(1);
            } catch (NotBoundException e) {
                log.fatal("Registry at "+cmd.getOptionValue("s")+" doesn't know object "+ SERVER_NAME, e);
                System.exit(1);
            }
            
       
    }

}
