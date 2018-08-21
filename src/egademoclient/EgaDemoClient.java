/*
 * Copyright 2014 EMBL-EBI.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package egademoclient;

import asg.cliche.Command;
import asg.cliche.ShellFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import static org.apache.commons.lang3.StringUtils.getLevenshteinDistance;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import uk.ac.embl.ebi.ega.filesystems.EgaMemoryCIPFuse;
import uk.ac.embl.ebi.ega.utils.EgaFile;
import uk.ac.embl.ebi.ega.utils.EgaTicket;
import utils.MyTimerTask;
import utils.Version;

/*
 * This is a demonstration of how the EGA Download API can be used to provide
 * a download client application with some advanced functionality.
 */

public class EgaDemoClient {
    private final String version = "2.2.3";
    private boolean newVersion = false;
    
    private static String infoServer = "ega.ebi.ac.uk";
    private static String dataServer = "ega.ebi.ac.uk"; //ega.ebi.ac.uk:8112";
    private static String dataServer2 = "ega.ebi.ac.uk"; //"xfer.crg.eu";
    private static final boolean ssl = true;
    private static int primaryServer = 1;

    private static final String dataServerTest = "pg-ega-pro-05.ebi.ac.uk:8111";

    private String globusPrefix = "/ega/rest/globus/v2";
    private String globusServer = "EGA-globus-server.ebi.ac.uk:8112";   
    
    private EgaDBAPIWrapper api; // Local SQLite DB enabled API Wrapper
    private long time;
    private double rate = -1.0;
    private int calibratedThreads = 3;
    private String myIp = null;
    private boolean useDB = false;
    
    private HashSet<String> legacy;
    private static boolean localstart = false;
    
    public EgaDemoClient(boolean db) {
        this.useDB = db; // Use local DB for tracking pending files
        
        // Test Version - Alert of there is a new version
        try {
            URL url = new URL("https://www.ebi.ac.uk/ega/sites/ebi.ac.uk.ega/files/documents/version.txt");
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setConnectTimeout(4000);
            conn.setReadTimeout(4000);
            InputStream is = null;
            try {
                is = conn.getInputStream();            
                Scanner s = new Scanner(is);
                String toString = s.nextLine(); 

                if (this.version.compareToIgnoreCase(toString)!=0) {
                    Version a = new Version(this.version);
                    Version b = new Version(toString);
                    if (b.compareTo(a) > 0) {
                        System.out.println("There is a new version of the EGA Demo Client available for download.");
                        System.out.println("Please update to the new version of the client before continuing.");
                        System.out.println("** New version: " + toString);
                        this.newVersion = true;
                    }
                }
                s.close();
            } finally {
                if (is!=null) is.close();
            }
        }
        catch(IOException ex) {
           //ex.printStackTrace(); // for now, ignore it.
        }        

        try {
            legacy = new HashSet<>();
            legacy.add("testme");

            if (!localstart) {
                URL url = new URL("https://www.ebi.ac.uk/ega/sites/ebi.ac.uk.ega/files/documents/legacy.txt");
                HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                conn.setConnectTimeout(4000);
                conn.setReadTimeout(4000);
                InputStream is = null;
                try {
                    is = conn.getInputStream();            
                    Scanner s = new Scanner(is);

                    while (s.hasNextLine()) {
                        String toString = s.nextLine(); 
                        legacy.add(toString);
                    }

                    s.close();
                } finally {
                    if (is!=null) is.close();
                }
            }
        }
        catch(IOException ex) {
           //ex.printStackTrace(); // for now, ignore it.
        }
        
        System.out.println("Ega Demo Download Client  Version: " + this.version); 
            // + "   With Ega Download Agent  Version: 0.7 BETA");
    }

    // -------------------------------------------------------------------------
    // Interactive Shell Commands! ---------------------------------------------
    // -------------------------------------------------------------------------
    
    @Command
    public String login(String username) {
        // Read password from console, characters hidden
        System.out.print("Password: ");
        char[] pw = System.console().readPassword();

        return login(username, new String(pw));
    }
    @Command
    public String login(String username, String password) {
        // Default: don't use DB
        return login(username, password, false);
    }
    @Command
    public String login(String username, String password, boolean useDB) {
        // Setup: primary and backup data sources
        if (this.api == null && EgaDemoClient.primaryServer == 1) {
            this.api = new EgaDBAPIWrapper(infoServer, dataServer, ssl, globusServer, globusPrefix); 
            this.api.setBackupDataServer(dataServer2);
        } else if (this.api == null && EgaDemoClient.primaryServer == 2) {
            this.api = new EgaDBAPIWrapper(infoServer, dataServer2, ssl, globusServer, globusPrefix);
            this.api.setBackupDataServer(dataServer);
        }
        
        if (this.api != null) {
            this.useDB = useDB;
            this.api.setDB(useDB);
        }
        
        int login_counter = 3; // Automatically re-try login function 3 times
        boolean login = false;
        while (!login && (login_counter-->0)) { // try 3 times to log in
            login = this.api.login(username, password.toCharArray(), useDB);
            if (!login)
                try {Thread.sleep(575);} catch (InterruptedException ex) {}
        }
        if (!login) {
            this.api = null;
            return "Login failed\n";
        }

        this.myIp = this.api.myIP();
        this.time = System.currentTimeMillis();
        return "Login Success!\n";
    }
    
    @Command
    public String logout(boolean verbose) {
        if (this.api!=null && this.api.session()) this.api.logout(verbose);
        return "Logout!\n";
    }
    @Command
    public String logout() {
        return logout(false);
    }
    
    // Globus Commands ---------------------
    @Command
    public String globuslogin(String globususername) {
        System.out.println("This command authenticates directly with Globus Online. EGA will not see these credentials.");
        
        // Read password from console, characters hidden
        System.out.print("Globus Password: ");
        char[] pw = System.console().readPassword();

        return globuslogin(globususername, new String(pw));
    }
    @Command
    public String globuslogin(String globususername, String globuspassword) {
        StringBuilder sb = new StringBuilder();        

        if (this.api == null) {
            this.api = new EgaDBAPIWrapper(infoServer, dataServer, ssl); 
            this.api.setBackupDataServer(dataServer2);
        }
        
        boolean success = this.api.globusLogin(globususername, globuspassword.toCharArray());

        if (success)
            sb.append("Globus Login Success!\n");
        else
            sb.append("Globus Login Failed!\n");

        return sb.toString();
    }
    
    @Command
    public String globustransfer(String endpoint, String request) {
        StringBuilder sb = new StringBuilder();        
        
        if (this.api == null) {
            this.api = new EgaDBAPIWrapper(infoServer, dataServer, ssl); 
            this.api.setBackupDataServer(dataServer2);
        }
        
        String globusStartTransfer = this.api.globusStartTransfer(request, endpoint);
        if (globusStartTransfer==null)
            sb.append("Log in to Globus Online first, using your Globus identity!");
        else
            sb.append(globusStartTransfer);
        
        return sb.toString();
    }
    // -------------------------------------
    
    @Command
    public String help() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        StringBuilder sb = new StringBuilder();        
        try {
            URL url = new URL("https://www.ebi.ac.uk/ega/sites/ebi.ac.uk.ega/files/documents/helptext.txt");
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(7000);
            InputStream is = conn.getInputStream();
            Scanner s = new Scanner(is);

            while (s.hasNextLine()) {
                sb.append(s.nextLine()).append("\n");
            }
            s.close();    
        }
        catch(IOException ex) {
           //ex.printStackTrace(); // for now, ignore it.
        }
        
        String text = sb.toString();
        if (text.length() == 0)
            text = "EGA Download help file is missing. Please contact the EGA Helpdesk.\n";
        
        return text;
    }
    
    @Command
    public String instructions() {
        StringBuilder sb = new StringBuilder();
        sb.append("  login {username} [{password}] [{use_database}] - to log in - password can be specified on the same line.\n");
        sb.append("  logout [{verbose}].\n");
        //sb.append("  overview - list overview of all requests.\n");
        sb.append("  path {path} - Set a working directory path, e.g. for downloads.\n");
        sb.append("  pwd - Current working directory path.\n");
        sb.append("  datasets - to list all permitted datasets.\n");
        sb.append("  files dataset {datasetid} - to list all files in dataset {datasetid}.\n");
        sb.append("  filedetails {fileid} - to list details about the file {fileid}.\n");
        sb.append("  size {type} {id} - total size {type='file'|'dataset'|'request'}.\n");
        sb.append("  requests - to list all download request labels.\n");
        sb.append("  requesttickets {label} - to list all download tickets in request {labels}.\n");
        //sb.append("  allrequests - to list all IPs from which download request were made\n");
        //sb.append("  localize {request} - to 'localize' a request's IP to the current IP (to enable download)\n");
        sb.append("  details {ticket} - to list details about a request ticket.\n");
        sb.append("  request {'dataset'|'file'} {id} {reKey} {label} - to request data for download.\n");
        sb.append("  [requestpending {label} {reKey} - to request pending files in {label} for download.]\n");
        sb.append("  download {label} [{parallel}] - to download a request (multiple files); by default 5 parallel streams [can be specified 1-15].\n");
        //sb.append("  downloadtocrg {label} {threads} - to download a request (multiple files) with the CRG Public Key.\n");
        sb.append("  ticketdownload {ticket} - to download one ticket (i.e. one file).\n");
        //sb.append("  downloadtonull {label} - to download a request (multiple files) wihout saving\n");
        //sb.append("  ticketdownloadtonull {ticket} - to download a file (i.e. a ticket) wihout saving it\n");
        sb.append("  deleterequest {label} - remove a request.\n");
        sb.append("  deleteticket {ticket} - remove a download ticket.\n");
        sb.append("  decrypt {filename} {key} - decrypt a downloaded file.\n");
        sb.append("  decryptkeep {filename} {key} - decrypt a downloaded file, and keep (don't delete) the encrypted file.\n");
        //sb.append("  testbandwidth [{threads}] - test connection speed.\n");
        //sb.append("  testbestdownload - try to determine best download parameters.\n");
        sb.append("  version - to show the version number of this shell.\n");
        sb.append("  verbose {true/false} - set level of output.\n");
        sb.append("  tutorial - quick instructions.\n");
        sb.append("  instructions - commands in the download client.\n");
        sb.append("  globuslogin {globus user} {globus password} - Authenticate your Globus User, directly with Globus.\n");
        sb.append("  globustransfer {endpoint} {request} - transfer specified request to your Globus endpoint.\n");
        sb.append("  downloadmetadata {dataset} - download metadata for specified dataset, if it exists.\n");
        sb.append("  exit - to quit the shell.\n");
        
        return sb.toString();
    }
    
    @Command
    public String tutorial() {
        StringBuilder sb = new StringBuilder();
        sb.append("Downloading data is a 2-step process: (1) Request and (2) Download.\n");
        sb.append("Before files can be downloaded they must be requested!\n\n");
        sb.append("(1) Requesting files using the 'request' command, for example:\n");
        sb.append("    'request dataset EDAD000102837 mysecretkey requestlabel'\n");
        sb.append("        where 'mysecretkey' is the encryption key of the received files\n");
        sb.append("        and 'requestlabel' is a label by which this request can be downloaded.\n");
        sb.append("    Using the command 'requests' then lists all current requests and the number of files in each request.\n\n");
        sb.append("(2) Downloading a previously made request using the 'download' command:\n");
        sb.append("    'download requestlabel'\n");
        sb.append("    This downloads the files contained in the request with the label 'requestlabel'\n");
        sb.append("\n");
        sb.append("During download files have the extension '.egastream', until the download process\n");
        sb.append("is complete and the file has been validated. At that point the extension is removed.\n");

        return sb.toString();
    }
    
    @Command
    public String version() {
        return "EGA Secure Data Shell Demo - Version: " + this.version + "\n";
    }

    @Command
    public String path(String path) { // Potential: Error Message
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        return "'" + path + "' set: " + this.api.setSetPath(path) + "\n";
    }
    
    @Command
    public String pwd() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        return "Current path: " + this.api.getPath() + "\n";
    }
    
    @Command
    public String verbose(String tag) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        if (tag.trim().equalsIgnoreCase("true") || tag.trim().equalsIgnoreCase("on"))
            this.api.setVerbose(true);
        if (tag.trim().equalsIgnoreCase("false") || tag.trim().equalsIgnoreCase("off"))
            this.api.setVerbose(false);
        
        return "Verbose set: " + this.api.getVerbose() + "\n";
    }
    
    @Command
    public String udt(String tag) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        if (tag.trim().equalsIgnoreCase("true") || tag.trim().equalsIgnoreCase("on"))
            this.api.setUdt(true);
        if (tag.trim().equalsIgnoreCase("false") || tag.trim().equalsIgnoreCase("off"))
            this.api.setUdt(false);
        
        return "UDT set: " + this.api.getUdt() + "\n";
    }

    @Command
    public String datasets() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        StringBuilder sb = new StringBuilder();
        String[] ds = this.api.listDatasets();
        try {
            for (int i=0; i<ds.length; i++)
                sb.append("  ").append(ds[i]).append("\n");
        } catch (Throwable t) {
            sb.append("No Datasets found\n");
        }
        
        return sb.toString();
    }
    
    @Command
    public String files(String type, String id) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        boolean dataset = type.equalsIgnoreCase("dataset");
        if (dataset) {
            if (this.legacy.contains(id)) {
                return "This is a legacy dataset. Please contact the EGA helpdesk for more information.\n";
            }
        }
        
        EgaFile[] ds = dataset?this.api.listDatasetFiles(id):null;
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Files in ").append(id).append(":\n");
            for (int i=0; i<ds.length; i++)
                sb.append("  ").append(ds[i].getFileName()).append("  ").append(ds[i].getFileSize()).append("  ").append(ds[i].getFileID()).append("  ").append(ds[i].getStatus()).append("\n");
        } catch (Throwable t) {
            sb.append("Request type: ").append(type).append(", ID: ").append(id).append(" produced no results\n");
        }
        return sb.toString();
    }
    
    @Command
    public String filedetails(String fileid) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaFile[] ds = this.api.listFileInfo(fileid);
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Details for ").append(fileid).append(":\n");
            for (int i=0; i<ds.length; i++)
                sb.append("  ").append(ds[i].getFileName()).append("  ").append(ds[i].getFileSize()).append("  ").append(ds[i].getFileID()).append("  ").append(ds[i].getStatus()).append("\n");
        } catch (Throwable t) {
            sb.append("Request for file: ").append(fileid).append(" produced no results\n");
        }
        
        return sb.toString();
    }

    @Command
    public String size(String type, String id) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        StringBuilder sb = new StringBuilder();
        long sze = 0;
        
        // Type of request - dataset, file, or request
        if (type.equalsIgnoreCase("file")) {
            EgaFile[] listFileInfo = this.api.listFileInfo(id);
            if (listFileInfo!=null && listFileInfo.length>0) {
                for (EgaFile x : listFileInfo)
                    if (x.getStatus().equalsIgnoreCase("available"))
                        sze += x.getFileSize();
            }
        } else if (type.equalsIgnoreCase("dataset")) {
            EgaFile[] listDatasetFiles = this.api.listDatasetFiles(id);
            if (listDatasetFiles!=null && listDatasetFiles.length>0) {
                for (EgaFile x : listDatasetFiles)
                    if (x.getStatus().equalsIgnoreCase("available"))
                        sze += x.getFileSize();
            }
        } else if (type.equalsIgnoreCase("request")) {
            EgaTicket[] listRequest = this.api.listRequest(id);
            if (listRequest!=null && listRequest.length>0) {
                for (EgaTicket x : listRequest)
                    sze += Long.parseLong(x.getFileSize());
            }
        } else {
            sb.append("Unknown type. Use 'file' or 'dataset' or 'request'.\n");
        }
        
        // Format Display based on total size
        int unit = 1024;
        long bytes = sze;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = ("KMGTPE").charAt(exp-1) + ("");
        String disp = String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);        
        
        sb.append("Size of " + type + " " + id + ": " + disp + "\n");
        
        return sb.toString();
    }    
        // -------------------------------------------------------------------------

    @Command
    public String requests() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        String[] ds = this.api.listAllRequestsLight();
        
        StringBuilder sb = new StringBuilder();
        if (ds!=null) {
            if (ds.length > 0)
                sb.append("Current Requests:\n");
            else
                sb.append("No Current Requests\n");
            for (int i=0; i<ds.length; i++)
                sb.append(ds[i]).append("\n");
        }
        sb.append("\n");
        
        return sb.toString();
    }
                
    @Command
    public String requestsx() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds = this.api.listAllRequests();
        
        StringBuilder sb = new StringBuilder();
        if (ds!=null) {
            try {
                ArrayList<String> reqs = new ArrayList<>();
                HashMap<String,Integer> reqcnt = new HashMap<>();

                for (int i=0; i<ds.length; i++) {
                    String req = ds[i].getLabel();
                    if (!reqs.contains(req)) {
                        reqs.add(req);
                        reqcnt.put(req, 1);
                    } else {
                        int y = reqcnt.get(req) + 1;
                        reqcnt.put(req, y);
                    }
                }
                if (reqs.size() > 0)
                    sb.append("Current Requests:\n");
                else
                    sb.append("No Current Requests\n");
                for (int i=0; i<reqs.size(); i++) {
                    sb.append("  ").append(reqs.get(i)).append("\t").append(reqcnt.get(reqs.get(i))).append("\n");
                }

            } catch (Throwable t) {
                sb.append("Error retrieving Requests.\n");
            }
        } else {
            sb.append("Error retrieving requests\n");
        }
        return sb.toString();
    }
    @Command
    public String allrequests() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds = this.api.listAllRequests();
        StringBuilder sb = new StringBuilder();
        try {
            ArrayList<String> reqs = new ArrayList<>();
            HashMap<String,Integer> reqcnt = new HashMap<>();
            sb.append("Current Requests from all Sources, with IP address at time of request:\n");
            for (int i=0; i<ds.length; i++) {
                String req = ds[i].getLabel();
                if (!reqs.contains(req)) {
                    reqs.add(req);
                    reqcnt.put(req, 1);
                } else {
                    int y = reqcnt.get(req) + 1;
                    reqcnt.put(req, y);
                }
            }
            for (int i=0; i<reqs.size(); i++) {
                sb.append("  ").append(reqs.get(i)).append("\t").append(reqcnt.get(reqs.get(i))).append("\n");
            }
        } catch (Throwable t) {
            sb.append("No Current Requests found\n");
        }
        return sb.toString();
    }
/*    
    @Command
    public String localize(String descriptor) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        String reply[] = this.api.localize(descriptor);
        
        return reply[0];
    }
*/    
    @Command
    public String requesttickets() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds = this.api.listAllRequests();
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Current Requests:\n");
            for (int i=0; i<ds.length; i++)
                sb.append("  ").append(ds[i].getTicket()).append("\t").append(ds[i].getLabel()).append("\n");
        } catch (Throwable t) {
            sb.append("No Current Requests found\n");
        }
        return sb.toString();
    }
    
    @Command
    public String requesttickets(String descriptor) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds = this.api.listRequest(descriptor);
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Current Requests:\n");
            for (int i=0; i<ds.length; i++)
                sb.append("  ").append(ds[i].getTicket()).append("\n");
        } catch (Throwable t) {
            sb.append("No Current Requests found\n");
        }
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------
    
    @Command
    public String details(String ticket) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds = this.api.listTicketDetails(ticket);
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Requests Details:\n");
            for (int i=0; i<ds.length; i++)
                sb.append("  Ticket: ").append(ds[i].getTicket()).append("\n")
                        .append("  File: ").append(ds[i].getFileName()).append("\n")
                        .append("  File Size: ").append(ds[i].getFileSize()).append("\n")
                        .append("  Request: ").append(ds[i].getLabel()).append("\n");
        } catch (Throwable t) {
            sb.append("No Details for request ").append(ticket).append(" found.\n");
        }
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------
    
    @Command
    public String request() {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        Scanner in = new Scanner(System.in);
        
        System.out.print("Request type ('file', 'packet', or 'dataset'): ");
        String type = in.next();
        
        System.out.print("Request ID (e.g. dataset ID, if 'dataset' is requested): ");
        String id = in.next();
        
        System.out.print("Re-Encryption key: ");
        String reKey = in.next();
        
        System.out.print("Description (no spaces): ");
        String descriptor = in.next();
        
        return request(type, id, reKey, descriptor);
    }
    @Command
    public String request(String type, String id, String reKey) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        Date dt = new Date();
        SimpleDateFormat sdf_new = new SimpleDateFormat("yyMMdd");
        String dateString = sdf_new.format(dt);
        String descriptor = type + "_" + id + "_" + dateString;
        
        return request(type, id, reKey, descriptor);
    }
    @Command
    public String request(String type, String id, String reKey, String descriptor) {
        return request(type, id, reKey, descriptor, "");
    }
    @Command
    public String request(String type, String id, String reKey, String descriptor, String target) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        boolean dataset = type.equalsIgnoreCase("dataset");
        if (dataset) {
            if (this.legacy.contains(id)) {
                return "This is a legacy dataset. Please contact the EGA helpdesk to request this data.\n";
            }
        }
        
        String[] ds = this.api.requestByID(id, type, reKey, descriptor, target);
        StringBuilder sb = new StringBuilder();
        try {
            if (ds.length == 1 && ds[0].equalsIgnoreCase("Access not Permitted")) {
                sb.append("You don't have authorization to request " + type + " " + id + ".\n\n");
            } else if (ds.length == 0 || (ds.length == 1 && ds[0].equalsIgnoreCase("-1"))) {
                sb.append("No file(s) to be requested for 'request " + type + " " + id + "'; file(s) may still be pending.\nPlease contact the EGA Helpdesk if you think this is an error.\n\n");
            } else {
                sb.append("Resulting Request:\n");
                int cnt = 0;
                for (int i=0; i<ds.length; i++) {
                    //sb.append("  ").append(ds[i]).append("\n");
                    cnt++;
                }
                if (cnt > 0)
                    sb.append("  ").append(descriptor).append("\t(").append(cnt).append(" new request(s)).\n");
                else {
                    sb.append("There are no files to be requested!\n");
                    sb.append("   This is not an error; not all datasets are available (yet) via the download API.\n");
                    sb.append("   Please contact the EGA helpdesk to download files in this dataset.\n");
                    //sb.append("   Possible causes: incorrect id, or requested files(s) are pending\n");
                    //sb.append("   (pending files have a length of -1 bytes and can't be downloaded yet).\n");
                }
            }
        } catch (Throwable t) {
            sb.append("Error requesting this!\n");
            sb.append("   Possible causes: incorrect id, or requested files(s) are pending\n");
            sb.append("   (pending files have a length of -1 bytes and can't be downloaded yet).\n");
        }
        return sb.toString();
    }
    @Command
    public String requestpending(String descriptor, String reKey) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
    
        String[] ds = this.api.requestPending(descriptor, reKey);
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("Resulting Request:\n");
            int cnt = 0;
            for (int i=0; i<ds.length; i++) {
                cnt++;
            }
            if (cnt > 0)
                sb.append("  ").append(descriptor).append("\t(").append(cnt).append(" file requests).\n");
            else {
                sb.append("There are no files to be requested: Files are still in 'pending' state!\n");
            }
        } catch (Throwable t) {
            sb.append("Error requesting this!\n");
        }
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------
    
    @Command
    public String download(String descriptor) {
        return download(descriptor, 5, "", true);
    }
    @Command
    public String downloadx(String descriptor) {
        return download(descriptor, 5, "", false);
    }
    //@Command
    //public String downloadtonull(String descriptor, int threads) {
    //    return download(descriptor, threads, "Null", true);
    //}
    //@Command
    //public String downloadtonull(String descriptor) {
    //    return download(descriptor, 5, "Null", true);
    //}
    @Command
    public String download(String descriptor, int threads) {
        return download(descriptor, threads, "", true);
    }
    @Command
    public String downloadx(String descriptor, int threads) {
        return download(descriptor, threads, "", false);
    }
    @Command
    public String download(String descriptor, int threads, String dev_null, boolean alt) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        StringBuilder sb = new StringBuilder();

        // Set Mode (false = Netty, true = HttpUrl) UDT must be Netty
        this.api.setAlt(alt);
        
        // Step 1: Get request tickets, remove descriptor
        EgaTicket[] ds = this.api.listRequest(descriptor);
        if (ds==null) {
            boolean failure = true;
            
            String[] listAllRequestsLight = this.api.listAllRequestsLight();
            
            // In case of error - match existing requests to provided request label; retry or list error
            if (listAllRequestsLight != null && listAllRequestsLight.length>0) {
                boolean match = false;
                CharSequence t = descriptor;
                for (int i=0; i<listAllRequestsLight.length; i++) {
                    CharSequence s = listAllRequestsLight[i];
                    int levenshteinDistance = getLevenshteinDistance(s, t);
                    if (!match && descriptor.equals(listAllRequestsLight[i])) match = true;
                    sb.append("Specified Request: ").append(descriptor).append("\tExisting Request (").append(listAllRequestsLight[i])
                            .append(i).append(")\t").append("Distance Score: ").append(levenshteinDistance).append("\n");
                }
                
                if (match) {
                    ds = this.api.listRequest(descriptor);
                    if (ds!=null && ds.length>0) {
                        failure = false;
                        sb = new StringBuilder();
                    }
                }
            } else            
                sb.append("Unable to retrieve tickets for request ").append(descriptor).append("\n");

            if (failure) return sb.toString();
        }
        // At this point ticket(s) successfully retrieved!
        
        // Filter out pending files (i.e. Null tickets)
        ArrayList<EgaTicket> ds_ = new ArrayList<>();
        for (int i=0; i<ds.length; i++) {
            if (ds[i].getTicket()!=null && ds[i].getTicket().length()>10)
                ds_.add(ds[i]);
        }
        System.out.println("Files to download in this request: " + ds_.size());
        ds = ds_.toArray(new EgaTicket[ds_.size()]);
        
        // Step 2: Parallel Download; {threads} threads
        try {  // 'Pool' uses Thread Pool, otherwise my own algorithm
            boolean dev_null_ = dev_null.equalsIgnoreCase("Null");
            runDownParRetry(ds, threads, dev_null_, descriptor);
        } catch (IOException ex) {
            Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Step 3: Return status
        sb.append("\n");
        
        // Done - return some text
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------

    @Command
    public String downloadtocrg(String descriptor) {
        return downloadtocrg(descriptor, 3);
    }
    
    @Command
    public String downloadtocrg(String descriptor, int threads) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        // Step 1: Get request tickets, remove descriptor
        EgaTicket[] ds = this.api.listRequest(descriptor);

        // Filter out pending files (i.e. Null tickets)
        ArrayList<EgaTicket> ds_ = new ArrayList<>();
        for (int i=0; i<ds.length; i++) {
            if (ds[i].getTicket()!=null && ds[i].getTicket().length()>10)
                ds_.add(ds[i]);
        }
        System.out.println("Files to download in this request: " + ds_.size());
        ds = ds_.toArray(new EgaTicket[ds_.size()]);
        
        // Step 2: Parallel Download; {threads} threads
        StringBuilder sb = new StringBuilder();
        
        try { // 'Pool' uses Thread Pool, otherwise my own algorithm
            runDownParRetry(ds, threads, false, descriptor);
            //runDownPar(ds, threads, false);
            //runDownParPool(ds, threads, false, false);
        } catch (IOException ex) {
            Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Step 3: Return status
        sb.append("Add more Info :) \n");
        
        // Done - return some text
        return sb.toString();
    }

    @Command
    public String downloadmetadata(String dataset) {
        StringBuilder sb = new StringBuilder();

        String[] downoad_metadata = this.api.downoad_metadata(dataset);
        
        for (String downoad_metadata1 : downoad_metadata) {
            sb.append(downoad_metadata1).append("\n");
        }
        
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------
    
    @Command
    public String ticketdownload(String ticket) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds_ = this.api.listTicketDetails(ticket);
        if (ds_==null || ds_.length==0)
             return "Can't find ticket " + ticket + "\n";
        System.out.println("Downloading File: " + ds_[0].getFileName());
        
        String down_name = ds_[0].getFileName().toLowerCase().replaceAll("/", "_");
        if (down_name.endsWith("gpg"))
            down_name = down_name.substring(0, down_name.length()-3) + "cip";
        System.out.println("Local download file: " + down_name);
        
        this.api.setVerbose(true); // to be used in pre-1.0.1 versions
        long time = System.currentTimeMillis();
        //String[] ds = this.api.download_ticket_url(ticket, down_name);
        String[] ds = this.api.download(ticket, down_name, "");
        time = System.currentTimeMillis() - time;
        long length = 0;
        if (ds!=null && ds.length > 0) {
            if (ds[0].equalsIgnoreCase("Ticket is Locked!"))
                length = -10;
            else
                length = (new File(ds[0])).length();
        }
        
        StringBuilder sb = new StringBuilder();
        if (length == -10) {
            sb.append("Error requesting this: ticket is locked!\n");
        } else {
            double rate = (length * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);
        
            try {
                sb.append("Downloaded :\n");
                for (int i=0; i<ds.length; i++)
                    sb.append("  ").append(ds[i]).append("\n");
                sb.append("Rate: ").append(rate).append(" MB/s\n");
            } catch (Throwable t) {
                sb.append("Error requesting this!\n");
            }
        }
        return sb.toString();
    }
    /*
    @Command
    public String ticketdownloadtonull(String ticket) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        EgaTicket[] ds_ = this.api.listTicketDetails(ticket);
        if (ds_==null || ds_.length==0)
            return "Can't find ticket " + ticket + "\n";
        System.out.println("Downloading File: " + ds_[0].getFileName() + " to null.");
        
        this.api.setVerbose(true); // to be used in pre-1.0.1 versions
        long time = System.currentTimeMillis();
        String[] ds = this.api.download(ticket, null, null);
        time = System.currentTimeMillis() - time;
        long length = 0;
        if (ds!=null && ds.length > 0) {
            if (ds[0].equalsIgnoreCase("Ticket is Locked!"))
                length = -10;
            else
                length = (Long.parseLong(ds[0]));
        }
        
        StringBuilder sb = new StringBuilder();
        if (length == -10) {
            sb.append("Error requesting this: ticket is locked!\n");
        } else {
            double rate = (length * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);

            try {
                sb.append("Downloaded :\n");
                sb.append("  ").append(ds_[4]).append("\n");
                sb.append("Rate: ").append(rate).append(" MB/s\n");
            } catch (Throwable t) {
                sb.append("Error requesting this!\n");
            }
        }
        return sb.toString();
    }
    */
    // -------------------------------------------------------------------------

    @Command
    public String topup(String type, String id, String reKey, String path) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";
        
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory())
            return "Directory " + path + " can't be read.\n";
                    
        // 1 - Read files from path
        File[] listFiles = dir.listFiles();
        HashSet<String> hs = new HashSet<>();
        for (int i=0; i<listFiles.length; i++) {
            if (listFiles[i].isFile()) {
                String name = listFiles[i].getName();
                if (name.toLowerCase().endsWith(".cip") || name.toLowerCase().endsWith(".gpg"))
                    name = name.substring(0, name.length()-4);
                hs.add(name);
            }
        }
        
        // 2 - Read remote files
        boolean dataset = type.equalsIgnoreCase("dataset");
        EgaFile[] ds = this.api.listDatasetFiles(id);
        
        // 3 - Compare lists - retain missing files
        ArrayList<String> download = new ArrayList<>();
        for (int i=0; i<ds.length; i++) {
            String down_name_ = ds[i].getFileName();
            String fileID = "";
            if (down_name_ != null && down_name_.contains("\t")) {
                if (down_name_.lastIndexOf("\t")+1 < down_name_.length())
                    fileID = down_name_.substring(down_name_.lastIndexOf("\t")+1).trim();
                down_name_ = down_name_.substring(0, down_name_.indexOf("\t")).trim();
            }
            if (down_name_ != null && (down_name_.toLowerCase().endsWith(".gpg") || 
                                       down_name_.toLowerCase().endsWith(".cip")))
                down_name_ = down_name_.substring(0, down_name_.length()-4);
            if (down_name_ != null)
                down_name_ = down_name_.replaceAll("/", "_");
            
            if (!hs.contains(down_name_)) {
                download.add(fileID);
            }
        }
        
        // 4 - download missing files
        StringBuilder sb = new StringBuilder();
        if (download!=null && download.size() > 0) {
            String descriptor = "topup_" + id;
            for (int i=0; i<download.size(); i++) {
                request("file", download.get(i), reKey, descriptor);
            }
            sb.append("Added " + download.size() + " files for download.\n");
            
            String download1 = download(descriptor);
            sb.append(download1);
        } else {
            sb.append("Path is already complete. No new files available.\n");
        }
        
        // 5 - done
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------

    @Command
    public String deleterequest(String descriptor) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        StringBuilder sb = new StringBuilder();
        
        String[] delete_request = this.api.delete_request(descriptor);
        
        sb.append("Deleted request run: " + delete_request[0] + "\n");
        
        return sb.toString();
    }
    
    @Command
    public String deleteticket(String request, String ticket) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        // Delete the specified ticket
        this.api.delete_ticket(request, ticket);
        
        // Step 2: Parallel Download; {threds} threads
        StringBuilder sb = new StringBuilder();
        sb.append("Ticket " + ticket + " deleted.");
        
        return sb.toString();
    }

    @Command
    public String testbestdownload() {
        // Idea: run a few pre-determined scenarios, pick the best one
        // 50MB streams; TCP 1, 3, 5, 10 streams; UDT 1, 3, 5 Streams
        String _1 = testbandwidth(1, false, 50);
        String _2 = testbandwidth(3, false, 50);
        String _3 = testbandwidth(5, false, 50);
        String _4 = testbandwidth(10, false, 50);
        String _5 = testbandwidth(1, true, 50);
        String _6 = testbandwidth(3, true, 50);
        String _7 = testbandwidth(5, true, 50);
        
        System.out.println("\n\n\nResults:\n--------");
        System.out.println(_1);
        System.out.println(_2);
        System.out.println(_3);
        System.out.println(_4);
        System.out.println(_5);
        System.out.println(_6);
        System.out.println(_7);
        
        return "\nTest Complete. User 'testbestlargedownload' to run this test for large file sizes.\n";
    }
    
    @Command
    public String testbestlargedownload() {
        // Idea: run a few pre-determined scenarios, pick the best one
        // 50MB streams; TCP 1, 3, 5, 10 streams; UDT 1, 3, 5 Streams
        String _1 = testbandwidth(1, false, 500);
        String _2 = testbandwidth(3, false, 500);
        String _3 = testbandwidth(5, false, 500);
        String _4 = testbandwidth(10, false, 500);
        String _5 = testbandwidth(1, true, 500);
        String _6 = testbandwidth(3, true, 500);
        String _7 = testbandwidth(5, true, 500);
        
        System.out.println("\n\n\nLarge Download Results:\n-----------------------");
        System.out.println(_1);
        System.out.println(_2);
        System.out.println(_3);
        System.out.println(_4);
        System.out.println(_5);
        System.out.println(_6);
        System.out.println(_7);
        
        return "\nTest Complete. User 'testbestlargedownload' to run this test for large file sizes.\n";
    }
    
    @Command
    public String testbandwidth() {
        return testbandwidth(3);
    }
    @Command
    public String testbandwidth(int threads) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        // Step 1: Create 3 virtual tickets
        Random rnd = new Random();
        String app = "SpeedTestRequest" + String.valueOf(rnd.nextInt());
        long sze = 0;
        for (int i=0; i<threads; i++) {
            request("teststream", "1073725440", "n", app);
            sze += 1073725440;
        }

        // Step 2: Download to Null
        long time = System.currentTimeMillis();
        //String x = downloadtonull(app, threads);
        String x = download(app, threads);
        time = System.currentTimeMillis()-time;
        this.rate = (sze/1024.0/1024.0) / (time/1000.0); // MB / s
        
        // Step 3: Parallel Download; {threds} threads
        StringBuilder sb = new StringBuilder();
        sb.append("Estimated Transfer Rate: " + this.rate + " MB/s");
        
        return sb.toString();
    }
    private String testbandwidth(int threads, boolean udt, long sizeMB) {
        if (this.api== null || !this.api.session())
            return "Log in first!\n";

        long size = sizeMB * 1024L * 1024L; // Turn MB into bytes
        
        // Step 1: Create 3 virtual tickets
        Random rnd = new Random();
        String app = "SpeedTestRequest" + String.valueOf(rnd.nextInt());
        long sze = 0;
        for (int i=0; i<threads; i++) {
            request("teststream", String.valueOf(size+17L), "n", app);
            sze += (size+17L);
        }

        // Step 2: Download to Null
        boolean saved = this.api.getUdt();
        this.api.setUdt(udt);
        long time = System.currentTimeMillis();
        //String x = downloadtonull(app, threads);
        String x = download(app, threads);
        time = System.currentTimeMillis()-time;
        this.api.setUdt(saved);
        this.rate = (sze/1024.0/1024.0) / (time/1000.0); // MB / s
        
        // Step 3: Parallel Download; {threds} threads
        StringBuilder sb = new StringBuilder();
        sb.append("Estimated Max. Transfer Rate for " + (udt?"UDT":"TCP") + " and " + threads + " parallel streams: " + this.rate + " MB/s");
        
        return sb.toString();
    }
    
    // -------------------------------------------------------------------------
    
    @Command
    public String decrypt(String file, String key) {
        return decrypt(file, key, null, true);
    }
    @Command
    public String decryptkeep(String file, String key) {
        return decrypt(file, key, null, false);
    }
    @Command
    public String decrypt(String file, String key, String path) {
        return decrypt(file, key, path, true);
    }
    @Command
    public String decryptkeep(String file, String key, String path) {
        return decrypt(file, key, path, false);
    }
    @Command
    public String decrypt(String file, String key, String path, boolean delete) {
        
        ArrayList<String> files = new ArrayList<>();
        if(file.contains(",")) { // Handle multiple files: comma-separator
            StringTokenizer token = new StringTokenizer(file, ",");
            while (token.hasMoreElements()) {
                String fle = token.nextToken();
                if (  (new File(fle)).exists()  )
                    files.add(fle);
                else
                    System.out.println("Specified file '" + fle + "' does not exist or can't be found. Skipping this file.");
            }
        } else {
            if (  (new File(file)).exists()  )
                files.add(file);
            else
                System.out.println("Specified file '" + file + "' does not exist or can't be found. Skipping this file.");
        }

        String pth = ".";
        if (path!=null)
            pth = path;

        if (files.size()>0) {
            System.out.println("Decrypting " + files.size() + " file(s).");
            this.api.decrypt(key, pth, files, 128, delete);
        } else {
            System.out.println("No files specified to be decrypted.");
        }
        
        return "Done!\n";
    }
        
    // -------------------------------------------------------------------------
    // Derived (Advanced) Functionality
    
    // -- Download Retries
    private void runDownParRetry(EgaTicket[] tickets, int numThreads, boolean dev_null, String descriptor) throws IOException {
        EgaTicket[] tickets_ = new EgaTicket[tickets.length];
        System.arraycopy(tickets, 0, tickets_, 0, tickets.length);
    
        int numTickets = 0, numTicketsPost = 0, count = 0;
        do {
            numTickets = tickets_.length;
            
            ArrayList<EgaTicket> t__ = new ArrayList<>();
            for (EgaTicket oneTicket:tickets_)
                t__.add(oneTicket);
            ArrayList<String> runDownParPool = runDownParPool(t__, numThreads, dev_null); // Returns list of successful tickets
            
            EgaTicket[] post_download_tickets_ = null; // this.api.listRequest(descriptor);
            numTicketsPost = (tickets_.length - runDownParPool.size());
            if (numTicketsPost > 0) {
                int index = 0;
                post_download_tickets_ = new EgaTicket[numTicketsPost];
                for (int fi=0; fi<tickets_.length; fi++) {
                    if (!runDownParPool.contains(tickets_[fi].getTicket()))
                        post_download_tickets_[index++] = tickets_[fi];
                }
            }
            if (post_download_tickets_ != null) { // Reset loop - only attempt failed tickets
                if (numTicketsPost > 0) {
                    tickets_ = new EgaTicket[numTicketsPost];
                    System.arraycopy(post_download_tickets_, 0, tickets_, 0, numTicketsPost);
                }
            } else {
                System.out.println("Post-Download: No outstanding Tickets");
            }

            // Repeat if there are still tickets in the request, and retry-count < 3
        } while (numTicketsPost > 0 && numTickets!=numTicketsPost && count++ < 3);
        
    }
    
    // -- Parallel Downloads (my own thread pool algorithm)
    /*
    private void runDownPar(EgaTicket[] tickets, int numThreads, boolean dev_null) throws IOException {
        numThreads = numThreads<=15?numThreads:15;

        // Step 1: Set up all tickets (i.e. get name of files to be downloaded
        System.out.println("Start Download Process");
        //EgaDemoClient_DownloadThread my_threads[] = new EgaDemoClient_DownloadThread[numThreads];
        EgaDemoClient_Download my_threads[] = new EgaDemoClient_Download[numThreads];
        int count = 0, savecount = 0, size = tickets.length;
        
        HashSet<String> reTried = new HashSet<>();

        boolean alive = true;
        do { // perform test for all selected files
            ArrayList indices = new ArrayList(); // indices of "free" threads
            alive = false;
            for (int i=0; i<numThreads; i++) { // find threads that have ended
                if ( (my_threads[i] != null) && (my_threads[i].isAlive()) ) {
                    alive = true;
                } else {
                    //indices.add(i); // 'free' thread slot
                    if (my_threads[i] != null) { // indicates completed Thread - post process
                        System.out.println("Thread " + my_threads[i].getIndex() + " completed.");
                        String _ticket = my_threads[i].getTicket();
                        String _name = my_threads[i].getDownName();
                        if (my_threads[i].getSuccess()) { // If the download succeeded
                            System.out.println(my_threads[i].getResult());
                            my_threads[i] = null; // free up thread slot
                            savecount++; // count completed threads
                            System.out.println("Completed Downloading File " + savecount + " of " + (size));
                            indices.add(i); // 'free' thread slot
                        } else if (!reTried.contains(_ticket)) { // try again!
                            System.out.println("Re-Try: " + _ticket + " : ");
                            reTried.add(_ticket);
                            my_threads[i] = new EgaDemoClient_Download(_ticket, i, size, this.api, dev_null, _name);
                            if (my_threads[i] != null) {
                                my_threads[i].start(); // Start the download thread
                            } else {
                                System.out.println("Error re-creating thread object for " + tickets[count].getTicket());
                                savecount++; // skip; count as 'completed' thread, so that process can complete
                                indices.add(i); // 'free' thread slot
                            }
                        } else { // Wipe clean, move on (for now)
                            my_threads[i] = null;
                            savecount++; // skip; count as 'completed' thread, so that process can complete
                            indices.add(i); // 'free' thread slot                            
                        }
                    } else
                        indices.add(i); // 'free' thread slot
                }
            }

            // Previous loop determined free threads; fill them in the next loop
            if (indices.size() > 0 && count < size) { // If there are open threads, then
                for (int i=0; i<indices.size(); i++) { // Fill all open spaces
                    if (count < size) { // Catch errors

                        // Index [0->numThreads-1] of this thread
                        int index = Integer.parseInt(indices.get(i).toString());
                        
                        // Instantiate download thread object for ticket:count [0->numTickets-1]
                        //my_threads[index] = new EgaDemoClient_DownloadThread(tickets[count].getTicket(), index, size, this.api, dev_null, tickets[count].getFileName());
                        my_threads[index] = new EgaDemoClient_Download(tickets[count].getTicket(), index, size, this.api, dev_null, tickets[count].getFileName());
                        
                        if (my_threads[index] != null) {
                            my_threads[index].start(); // Start the download thread
                        } else {
                            System.out.println("Error creating thread object for " + tickets[count].getTicket());
                            savecount++; // skip; count as 'completed' thread, so that process can complete
                        }
                        count++; // count started threads
                        System.out.println("Started Downloading File " + count + " of " + (size));
                    }
                }
            }

            // runs until the number of completed threads equals the number of files, and all threads completed (redundant)
        }  while ((savecount < size) || alive);

        System.out.println("Download Attempts Completed.");
    }
    */
    // Java Thread Pool executor, taking care of executing all download tickets
    private ArrayList<String> runDownParPool(ArrayList<EgaTicket> t__, int numThreads, boolean dev_null) throws IOException {
        numThreads = numThreads<=15?numThreads:15;
        System.out.println("Start Download Process: " + numThreads + " (max:15) parallel threads");
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);        
        ArrayList<EgaTicket> t = (ArrayList<EgaTicket>) t__.clone(); // Work on a copy of the list
        ArrayList<String> result = new ArrayList<>();
        
        int countdown = 5;

        Future[] f = null;        
        TimerTask timerTask = new MyTimerTask(f);
        Timer  theTimer = new Timer(true);
        theTimer.scheduleAtFixedRate(timerTask, 30000, 30000);
        
        while (countdown-- > 0 && t.size() > 0) { // try up to 5 times: download all/failed tickets
            ArrayList<EgaTicket> t_ = new ArrayList<>(); // Collect unsucessful downloads
            System.out.println("Iteration " + (5-countdown) + ": " + t.size() + " files.");
            
            f = new Future[t.size()]; // ALL tickets
            ((MyTimerTask)timerTask).setF(f); // Does this work??
            for (int i=0; i<t.size(); i++) { // Process all tickets in parallel
                if (t.get(i).getTicket() == null || t.get(i).getFileName() == null) {
                    System.out.println("Download Ticket ERROR");
                } else {
                    //Callable worker = new EgaDemoClient_DownloadCallable(t.get(i).getTicket(), i, t.size(), this.api, dev_null, t.get(i).getFileName());
                    Callable worker = new EgaDemoClient_Download(t.get(i).getTicket(), i, t.size(), this.api, dev_null, t.get(i).getFileName());
                    f[i] = executorService.submit(worker); // Submit downloads in sequence
                }
            }
            // The queue is now set up - all requests are submitted!            
    
            // Results are printed in sequence, even if the download happens out-of-order
            for (int i=0; i<f.length; i++) { // get result for each download attempt, in sequence
                try {
                    String stat = f[i].get().toString(); // Error...
                    if (stat.startsWith("Download Failed") || stat.equalsIgnoreCase("Download Failed")) { // add failed attempts to new list
                        t_.add(t.get(i));
                    } else {// otherwise, print result
                        System.out.println(stat);
                        result.add(t.get(i).getTicket()); // Add completed ticket
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    System.out.println("["+i+"] " + ex.getLocalizedMessage());
                    t_.add(t.get(i));
                } catch (Throwable ex) {
                    // Don't re-try ... for now
                    System.out.println("Throwable error: " + ex.getLocalizedMessage());
                }
            }

            System.out.println("Iteration " + (5 - countdown) + " Done! (FAILED: " + t_.size() + "; SUCCEEDED: " + (t.size()-t_.size()) + ")");
            t = t_; // replace original list with list of failed downloads
        }
        
        if (theTimer!=null) theTimer.cancel();
        if (timerTask!=null) timerTask.cancel();
        theTimer = null;
        executorService.shutdown(); // Done - shut down executor
        while (!executorService.isTerminated()) {
            try {Thread.sleep(1000);} catch (InterruptedException ex) {;}
        }
        System.out.println("Download Attempt Completed. " + result.size() + " of " + t__.size() + " tickets downloaded successfully.");
        
        return result;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // Command line Options ----------------------------------------------------
    // This section merely allows calling the shell functions directly from the
    // command line. It uses the same code/functionality as the interactive shell
    private static void processCmd(EgaDemoClient shell, String[] args) {
        Options options = new Options();
        HelpFormatter formatter = new HelpFormatter();

        // Print a list of commands/options
        options.addOption("help", false, "help");
        
        // List Datasets
        // List Files in a Dataset
        options.addOption("ld", "listdatasets", false, "list datasets");
        options.addOption("lfd", "listdatasetfiles", true, "list files in dataset");
        options.addOption("lr", "listrequests", false, "list requests");
        options.addOption("lar", "listallrequests", false, "list requests from all IPs");
        options.addOption("lrt", "listrequesttickets", true, "list request tickets");
        
        // Sizes
        options.addOption("sf", "filesize", true, "list file size");
        options.addOption("sd", "datasetsize", true, "list size of dataset");
        options.addOption("sr", "requestsize", true, "list size of request");
        
        // Request File/Teststream/Packet/Dataset
        options.addOption("rf", "requestbyfileid", true, "request file by ID");
        options.addOption("rfd", "requestbydatasetid", true, "request files by dataset ID");
        //options.addOption("rt", "requestteststream", true, "request test stream");
        options.addOption("re", "reencryptionkey", true, "reencryption key");
        
        // Download Ticket
        // Download File/Teststream/Packet/Dataset 
        options.addOption("dt", "downloadticket", true, "download ticket");
        options.addOption("dr", "downloadrequest", true, "download request");
        //options.addOption("dtn", "downloadtickettonull", true, "download ticket to null");
        //options.addOption("drn", "downloadrequesttonull", true, "download request to null");
        
        // Delete
        options.addOption("delr","deleterequest",  true, "delete a request");
        options.addOption("delt","deleteticket",  true, "delete a ticket");        
        
        // Localize
        //options.addOption("llz", "localizerequest", true, "localize request to current IP");
        
        // Decryption of downloaded files
        Option opt = new Option("dc", "decrypt", true, "decrypt file(s)");
        opt.setArgs(100);
        options.addOption(opt);
        //options.addOption("dc", "decrypt", true, "decrypt file(s)");
        options.addOption("dck", "decryptionkey", true, "decryption key");

        // Options
        options.addOption("nt", "numthreads", true, "number of threads");
        options.addOption("path", true, "set path");
        options.addOption("label", true, "set label for downoad requests");
        options.addOption("crg", false, "download from CRG");
        options.addOption("nvb", false, "turn off verbose mode");
        //options.addOption("ndb", false, "turn off local database cache");
        options.addOption("usedb", false, "turn on local database cache");
        //options.addOption("updatedb", false, "update local database cache");
        options.addOption("udt", false, "pick UDT for downloads");

        // Other
        //options.addOption("overview", false, "Overview of current requests");
        //options.addOption("dltest", false, "Run quick test of prameter settings");
        //options.addOption("dltestlarge", false, "Run quick test of prameter settings, large files");
        
        // Parse command line argument list according to the above declared options
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse( options, args);

            if (cmd.hasOption("crg")) {
                EgaDemoClient.primaryServer = 2;
            }
            if (cmd.hasOption("nvb")) {
                System.out.println(shell.verbose("false"));
            } else {
                System.out.println(shell.verbose("true"));
            }
            if (cmd.hasOption("udt")) {
                System.out.println(shell.udt("true"));
            }
            
            if (cmd.hasOption("help")) {
                formatter.printHelp( "java -jar EgaDemoClient.jar", options, true );            
                return; // end after printing help list
            }
            
            int threads = 3;
            if (cmd.hasOption("nt")) // Number of parallel threads
                threads = Integer.parseInt(cmd.getOptionValue("nt").trim());

            String label = "";
            if (cmd.hasOption("label")) // Number of parallel threads
                label = cmd.getOptionValue("label").trim();
            
            // Decryption
            String[] file = null;
            if (cmd.hasOption("dc")) {
                file = cmd.getOptionValues("dc");
                
                String key="";
                if (cmd.hasOption("dck")) // Decryption key
                    key = cmd.getOptionValue("dck");

                String pth="";
                if (cmd.hasOption("path"))
                    pth = cmd.getOptionValue("path");
                
                for (String file1 : file) {
                    shell.decrypt(file1, key, pth);
                }                
                return; // End after decryption
            }
            
            // Delete
            if (cmd.hasOption("delr")) {
                String request = cmd.getOptionValue("delr");
                System.out.println(shell.deleterequest(request));
                return;
            }
            
            // Localize
/*            
            if (cmd.hasOption("llz")) {
                String request = cmd.getOptionValue("llz");
                System.out.println(shell.localize(request));
                return;
            }
*/            
            // List options - just print results to screen
            if (cmd.hasOption("ld"))
                System.out.println(shell.datasets());
            if (cmd.hasOption("lfd"))
                System.out.println(shell.files("dataset", cmd.getOptionValue("lfd")));
            if (cmd.hasOption("lr"))
                System.out.println(shell.requests());
            if (cmd.hasOption("lar"))
                System.out.println(shell.allrequests());
            if (cmd.hasOption("lrt"))
                System.out.println(shell.requesttickets(cmd.getOptionValue("lrt")));
            
            // Request options - just print resulting tickets to screen
            if (cmd.hasOption("re")) {
                String reKey = cmd.getOptionValue("re");

                // Request
                if (cmd.hasOption("rf"))
                    System.out.println(shell.request("file", cmd.getOptionValue("rf"), reKey, label));
                if (cmd.hasOption("rfd"))
                    System.out.println(shell.request("dataset", cmd.getOptionValue("rfd"), reKey, label));
                if (cmd.hasOption("rt"))
                    System.out.println(shell.request("teststream", cmd.getOptionValue("rt"), reKey, label));

            }
            
            // Size options
            if (cmd.hasOption("sf"))
                System.out.println(shell.size("file", cmd.getOptionValue("sf")));
            if (cmd.hasOption("sd"))
                System.out.println(shell.size("dataset", cmd.getOptionValue("sd")));
            if (cmd.hasOption("sr"))
                System.out.println(shell.size("request", cmd.getOptionValue("sr")));
                        
            // Download options
            if (cmd.hasOption("path"))
                System.out.println("Path set: " + shell.path(cmd.getOptionValue("path")));
            
            // Download by tickets
            if (cmd.hasOption("dt"))
                System.out.println(shell.ticketdownload(cmd.getOptionValue("dt")));
            if (cmd.hasOption("dr"))
                System.out.println(shell.download(cmd.getOptionValue("dr"), threads));
            //if (cmd.hasOption("dtn"))
            //    System.out.println(shell.ticketdownloadtonull(cmd.getOptionValue("dtn")));
            //if (cmd.hasOption("drn"))
            //    System.out.println(shell.downloadtonull(cmd.getOptionValue("drn"), threads));

            // Tests
            //if (cmd.hasOption("overview"))
            //    System.out.println(shell.overview());
            //if (cmd.hasOption("dltest"))
            //    System.out.println(shell.testbestdownload());
            //if (cmd.hasOption("dltestlarge"))
            //    System.out.println(shell.testbestlargedownload());
            
        } catch (ParseException ex) {
            formatter.printHelp( "java -jar EgaDemoClient.jar", options, true );            
            System.out.println("Unrecognized Parameter. " + ex.getLocalizedMessage());
            Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------

    private static void debug(String u, String p) {
        try {
            EgaTroubleshooter x = new EgaTroubleshooter(u, p);
            
            if(x.AccessTests()) {                
                if (x.AccessPing()) {
                    if (x.AccessLogin()) {
                        // OK -- basic tests done. Now test connection speed
                        
                        //try {
                        //    System.out.println("----Speed Test 1M 1 Thread, main server, tcp");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 1, 1000000);
                        //    System.out.println("----Speed Test 1M 5 Threads, main server, tcp");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 5, 1000000);
                        //    System.out.println("----Speed Test 100M 1 Thread, main server, tcp");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 1, 100000000);
                        //    System.out.println("----Speed Test 100M 5 Threads, main server, tcp");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 5, 100000000);
                        //} catch (IOException ex) {
                        //    Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
                        //    return;
                        //}
                        
                        //try {
                        //    System.out.println("----Speed Test 1M 1 Thread, main server, udt");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 1, 1000000, true);
                        //    System.out.println("----Speed Test 1M 5 Threads, main server, udt");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 5, 1000000, true);
                        //    System.out.println("----Speed Test 100M 1 Thread, main server, udt");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 1, 100000000, true);
                        //    System.out.println("----Speed Test 100M 5 Threads, main server, udt");
                        //    x.AccessSpeed("ega.ebi.ac.uk", 5, 100000000, true);
                        //} catch (IOException ex) {
                        //    Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
                        //    return;
                        //}
                        
                        // Done.
                        System.out.println("Debug protocol completed!");
                    } else
                        System.out.println("Can't log in to EGA Service. Is the username/password correct?\nIf this persists, please contact the EGA Helpdesk.)");
                } else
                    System.out.println("EGA Servers not reachable.");
            } else
                System.out.println("Local Firewall appears to block Java from accessing the Internet.");
            
            return;
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            Logger.getLogger(EgaDemoClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return;
    }
    
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws IOException {
        
        // Limit to 1 instance per user
        try {
            ServerSocket serverSocket = new ServerSocket(65416, 1, InetAddress.getByName(null));
        } catch (Throwable th) {
            System.out.println("Only 1 instance of the download client should be used at a time.\n");
            //return;
        }
        
        // process startup parameters
        // If the first parameter is '-p' then process command line arguments
        if (args.length > 0 && (args[0].equalsIgnoreCase("-crg") || (args[0].endsWith("crg")))) {
            EgaDemoClient.primaryServer = 2;
        }
        if (args.length > 0 && args[0].equalsIgnoreCase("-test")) {
            EgaDemoClient.dataServer = EgaDemoClient.dataServerTest;
            EgaDemoClient.dataServer2 = EgaDemoClient.dataServerTest;
            System.out.println("Entered Test Mode!");
        }
        if (args.length > 0 && args[0].equalsIgnoreCase("-local")) {
            EgaDemoClient.localstart = true;
            EgaDemoClient.dataServer = args[2];
            EgaDemoClient.dataServer2 = args[2];
            EgaDemoClient.infoServer = args[1];
            System.out.println("Entered Local Mode!");
            System.out.println("Info = " + EgaDemoClient.infoServer);
            System.out.println("Data = " + EgaDemoClient.dataServer);
        }
        if (args.length > 0 && (args[0].equalsIgnoreCase("-help"))) {
            System.out.println("Type 'java -jar EgaDemoClient.jar' for interactive shell.");
            System.out.println("Type 'java -jar EgaDemoClient.jar -p {username} {password} [-help]' for command line shell.");
            System.out.println("    Use '-pf {filepath}' to read username/password from a file.");
            System.out.println("    Use '-pfs {filepath}' to read username/password, plus all commands from a file.");
        } else if (args.length > 0 && args[0].equalsIgnoreCase("-version")) {
            new EgaDemoClient(false);
        } else if (args.length > 0 && args[0].equalsIgnoreCase("-debug")) {
            debug(args[1], args[2]);
            System.out.println("Exit");
            return;
        } else if (args.length == 4 && args[0].equalsIgnoreCase("-fuse")) {
            String source = args[1];
            String mount = args[2];
            String password = args[3];
            int AES_bits = 128;
            
            EgaMemoryCIPFuse fs = new EgaMemoryCIPFuse(source, mount, password, AES_bits);
            fs.run();
        } else if (args.length > 0 && (args[0].equalsIgnoreCase("-p") || args[0].equalsIgnoreCase("-pf") || args[0].equalsIgnoreCase("-pfs"))) {
            String u, p;
            int offset = 1;
            String[] args__ = null;
            if (args[0].equalsIgnoreCase("-pf") || args[0].equalsIgnoreCase("-pfs")) { // Get username/password from file
                BufferedReader bfr = new BufferedReader(new FileReader(args[1]));
                u = bfr.readLine().trim();
                p = bfr.readLine().trim();
                if (args[0].equalsIgnoreCase("-pfs")) { // read parameters from a file - one parameter per line
                    ArrayList<String> args_array = new ArrayList<>();
                    String temp;
                    while ( (temp=bfr.readLine()) != null  )
                        args_array.add(temp);
                    args__ = args_array.toArray(new String[args_array.size()]);
                }
                bfr.close();
                offset = 2;
            } else { // get username/password from parameter list
                u = args[1].trim();
                p = args[2].trim();
                offset = 3;
            }
            
            // Log in - no need to proceed if login fails
            EgaDemoClient shell = null;
            try {
                shell = new EgaDemoClient(false); // don't use db for cmd line (for speed)
                String login = shell.login(u, p, false);

                if (login.contains("Login Success!")) {
                    String[] args_ = new String[args__==null?args.length-offset:args__.length];
                    if (args__!=null) System.arraycopy(args__, 0, args_, 0, args_.length);
                    else System.arraycopy(args, offset, args_, 0, args_.length);
                    processCmd(shell, args_); // strip the leading '-p...' parameter
                } else {
                    System.out.println(login);
                    System.out.println(shell.logout(false));
                    shell = null;
                    System.exit(401);
                    return;
                }
            } finally {
                if (shell!=null)
                    System.out.println(shell.logout(false));
            }
        } else {
            // Otherwise run the interactice shell
            System.out.println("Welcome to the EGA Secure Data Shell Demo.");
            System.out.println("Type 'help' for help, and 'exit' to quit.");

            EgaDemoClient shell = null;
            try {
                shell = new EgaDemoClient(true);
                ShellFactory.createConsoleShell("EGA ", "", shell).commandLoop();
            } finally {
                if (shell!=null)
                    System.out.println(shell.logout(false));
            }
        }
        
        System.out.println("Exit");
        return;
    }
    
    // -------------------------------------------------------------------------
    public static String getInfoServer() {
        return infoServer;
    }
    public static String getDataServer() {
        return dataServer;
    }
    public static String getBackupDataServer() {
        return dataServer2;
    }    
    public static String getDataServerTest() {
        return dataServerTest;
    }
}
