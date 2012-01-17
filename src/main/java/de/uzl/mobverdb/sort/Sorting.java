package de.uzl.mobverdb.sort;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import de.uzl.utils.Files;

public class Sorting {
    
    private final static Logger log = Logger.getLogger(Sorting.class.getCanonicalName());
    
    public static enum Sorttype { MERGE, DIST, LOCAL };
    

    public static void main(String[] args) {
        Options options = new Options();
        OptionGroup mode = new OptionGroup();
        mode.addOption(new Option("s", "server", true, "Act as a server. Parameter: number of clients to wait for"));
        mode.addOption(new Option("c", "client", true, "Act as a client. Parameter: IP/hostname of the server (optionally with :port)"));
        options.addOptionGroup(mode);
        
        OptionGroup serverType = new OptionGroup();
        serverType.addOption(new Option("d", "distsort", false, "Use distributionsort (otherwise mergesort is used)"));
        serverType.addOption(new Option("l", "localsort", false, "Use a local sorting (otherwise mergesort is used)"));
        options.addOptionGroup(serverType);
        options.addOption(new Option("b", "blocksize", true, "Blocksize for data fetching from client (default: 10)"));
        CommandLineParser parser = new PosixParser();
        
        try {
            setupAndRun(parser.parse(options, args));
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(e.getMessage()+"\n");
            formatter.printHelp("sorting.jar <input file>", options);
        }
    }
    
    /**
     * Do the setup and run the app
     * @param cmd the parsed CommandLine
     * @throws ParseException on errors
     */
    private static void setupAndRun(CommandLine cmd) throws ParseException {
            try {
                if(cmd.hasOption('s')) {
                    if(cmd.getArgList().size() == 0) {
                        throw new ParseException("You must give a input file as paramter");
                    }
                    int numClients = Integer.parseInt(cmd.getOptionValue('s'));
                    
                    int blockSize = 10;
                    if(cmd.hasOption('b')) {
                        blockSize = Integer.parseInt(cmd.getOptionValue('b'));
                    }
                    Sorttype type = Sorttype.MERGE;
                    if(cmd.hasOption('d')) type = Sorttype.DIST;
                    if(cmd.hasOption('l')) type = Sorttype.LOCAL;
                    
                    serverMode(numClients, type, cmd.getArgs()[0], blockSize);
                } else if (cmd.hasOption('c')) {
                    clientMode("//"+cmd.getOptionValue("c")+"/"+SortServer.SERVER_NAME);
                } else {
                    throw new ParseException("You must specify which mode to use (client or server)");
                }
            } catch(NumberFormatException e) {
                throw new ParseException(e.getMessage());
            } catch (MalformedURLException e) {
                throw new ParseException(e.getMessage());
            } catch (NotBoundException e) {
                log.fatal("Registry at "+cmd.getOptionValue("c")+" doesn't know object "+ SortServer.SERVER_NAME, e);
                System.exit(1);
            }
    }
    
    private static void serverMode(int numClients, Sorttype sortType, String inputFile, int blockSize) {
        try {
            String inputData = Files.readFile(inputFile);
            SortServer s = new SortServer(numClients, sortType, inputData, blockSize);
            s.start();
        } catch (IOException e) {
            log.fatal(String.format("Input file '%s' not found.", inputFile));
        } 
    }
    
    private static void clientMode(String url) throws MalformedURLException, NotBoundException {
        try {
            SortingClient client = new SortingClient(url);
            client.start();
        } catch (RemoteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }
}
