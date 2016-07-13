/*
 * Copyright 2015 EMBL-EBI.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONObject;
import us.monoid.web.JSONResource;
import us.monoid.web.Resty;
import static us.monoid.web.Resty.content;
import static us.monoid.web.Resty.data;
import static us.monoid.web.Resty.form;

/**
 *
 * @author asenf
 */
public class EgaTroubleshooter {
    private String username, password;
    private TrustManager[] trustAllCerts;
    private HostnameVerifier allHostsValid;
    private EgaDBAPIWrapper api;
    
    public EgaTroubleshooter(String u, String p) throws NoSuchAlgorithmException, KeyManagementException {
        this.username = u;
        this.password = p;
        
        this.api = null;
        
        trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        } };
        
        // Install the all-trusting trust manager
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // Create all-trusting host name verifier
        allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };       
    }

    // Test if Java can access "reliable" web sites, using http and https
    public boolean AccessTests() {
        // *********************************************************************
        // Try "reliable" site http
        System.out.println("Try connecting to http Google");
        try {
            Socket socket = new Socket("www.google.com", 80);
            System.out.println("-1- " + socket.toString());
        } catch (IOException e) {
            System.out.println("Connect Error http: " + e.getLocalizedMessage());
            return false;
        }

        // *********************************************************************
        // Try "reliable" site https
        System.out.println("Try connecting to https Google");
        try {
            SSLSocketFactory factory=(SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLSocket sslsocket=(SSLSocket) factory.createSocket("www.google.com",443);
            System.out.println("-2- " + sslsocket.toString());
        } catch (IOException ex) {
            Logger.getLogger(EgaTroubleshooter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }
    
    // Test if EGA can be reached and pinged
    public boolean AccessPing() {
        // *********************************************************************
        System.out.println("Reaching EGA");
        try {
            InetAddress address = InetAddress.getByName("ega.ebi.ac.uk");
            System.out.println("Name: " + address.getHostName());
            System.out.println("Addr: " + address.getHostAddress());
            //System.out.println("Reach: " + address.isReachable(5000));
        }
        catch (UnknownHostException e) {
            System.err.println("Unable to lookup ega.ebi.ac.uk");
            return false;
        }
        //catch (IOException e) {
        //    System.err.println("Unable to reach ega.ebi.ac.uk");
        //}
        
        try {
            boolean ping = ping("ega.ebi.ac.uk");
            System.out.println("Pinging EGA: " + ping);
        } catch (IOException ex) {
            Logger.getLogger(EgaTroubleshooter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (InterruptedException ex) {
            Logger.getLogger(EgaTroubleshooter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
       
        return true;
    }
    
    private static boolean ping(String host) throws IOException, InterruptedException {
        boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");

        ProcessBuilder processBuilder = new ProcessBuilder("ping", isWindows? "-n" : "-c", "1", host);
        Process proc = processBuilder.start();

        int returnVal = proc.waitFor();
        return returnVal == 0;
    }
    
    public boolean AccessLogin() {
        // *********************************************************************
        System.out.println("EGA Login Attempt");        
        // Existing login
        try {
            Resty r = new Resty();
            
            JSONObject json = new JSONObject();
            json.put("username", URLEncoder.encode(username,"UTF-8"));
            json.put("password", URLEncoder.encode(new String(password),"UTF-8"));
            
            String url = "https://ega.ebi.ac.uk:443/ega/rest/access/v2/users/login";
            
            JSONResource json1 = r.json(url, form( data("loginrequest", content(json)) ));
            JSONObject jobj = (JSONObject) json1.get("response");
            JSONArray jsonarr = (JSONArray)jobj.get("result");
            
            
            String result = jsonarr.length()>0?jsonarr.getString(0):"";
            String sessiontoken = "";            
            if (!result.toLowerCase().startsWith("success")) {
                System.out.println("-4- " + "Error: " + result);
                return false;
            } else {
                sessiontoken = jsonarr.length()>1?jsonarr.getString(1):"";
                System.out.println("-4- " + "Success: " + sessiontoken);
            }
            
        } catch (Exception ex) {
            System.out.println(ex.toString());
            return false;
        }
        
        // *********************************************************************
        
        return true;
    }
    
    public boolean AccessSpeed(String dataserver, int threads, long size) throws IOException {
        return AccessSpeed(dataserver, threads, size, false);
    }
    public boolean AccessSpeed(String dataserver, int threads, long size, boolean udt) throws IOException {
        this.api = new EgaDBAPIWrapper("ega.ebi.ac.uk", dataserver, true);
        
        boolean login = this.api.login(this.username, this.password.toCharArray());
        if (login) {
            if (udt) this.api.setUdt(true);
            
            double rate = 0;
            // Step 1: Create 3 virtual tickets
            Random rnd = new Random();
            String app = "SpeedTestDebug" + String.valueOf(rnd.nextInt());
            long sze = 0;
            String[] dss = new String[threads];
            //ArrayList<String> dsl = new ArrayList<String>();
            for (int i=0; i<threads; i++) {
                String[] ds = this.api.requestByID(String.valueOf(size), "teststream", "n", app, "");
                //dsl.add(ds[0]);
                dss[i] = ds[0].contains("\t")?ds[0].substring(0,ds[0].indexOf("\t")):ds[0];
                sze += size;
            }

            // Step 2: Download to Null
            long time = System.currentTimeMillis();
            runDownPar(dss, threads, true);
            time = System.currentTimeMillis()-time;
            rate = (sze/1024.0/1024.0) / (time/1000.0); // MB / s

            // Step 3: Parallel Download; {threds} threads
            System.out.println("Estimated Transfer Rate: " + rate + " MB/s");
        } else
            return false;
        
        return login;
    }

   // Java Thread Pool executor, taking care of executing all download tickets
    private ArrayList<String> runDownParPool(ArrayList<String> t__, int numThreads) throws IOException {
        boolean dev_null = true;
        numThreads = numThreads<=10?numThreads:10;
        System.out.println("Start Download Process: " + numThreads + " (max:10) parallel threads");
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);        
        ArrayList<String> t = (ArrayList<String>) t__.clone(); // Work on a copy of the list
        ArrayList<String> result = new ArrayList<>();
        
        int countdown = 100;
        
        while (countdown-- > 0 && t.size() > 0) { // try up to 100 times: download all/failed tickets
            ArrayList<String> t_ = new ArrayList<String>(); // Collect unsucessful downloads
            
            Future[] f = new Future[t.size()]; // ALL tickets
            for (int i=0; i<t.size(); i++) { // Process all tickets in parallel
                Callable worker = new EgaDemoClient_Download(t.get(i), i, t.size(), this.api, dev_null, String.valueOf(i));
                f[i] = executorService.submit(worker); // Submit downloads in sequence
            }
            for (int i=0; i<f.length; i++) { // get result for each download attempt, in sequence
                try {
                    String stat = f[i].get().toString(); // Error...
                    if (stat.equalsIgnoreCase("Download Failed.")) { // add failed attempts to new list
                        t_.add(t.get(i));
                    } else {// otherwise, print result
                        System.out.println(stat);
                        result.add(t.get(i)); // Add completed ticket
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    System.out.println("["+i+"] " + ex.getLocalizedMessage());
                    t_.add(t.get(i));
                } catch (Throwable ex) {
                    // Don't re-try ... for now
                    System.out.println("Throwable error: " + ex.getLocalizedMessage());
                }
            }
            t = t_; // replace original list with list of failed downloads
            System.out.println("Iteration Done! (" + t.size() + " / " + t__.size() + ")");
        }
        
        executorService.shutdown(); // Done - shut down executor
        while (!executorService.isTerminated()) {
        }
        System.out.println("Download Process Completed. (" + (100-countdown) + " iterations)");
        
        return result;
    }

    private void runDownPar(String[] tickets, int numThreads, boolean dev_null) throws IOException {

        System.out.println("Start Download Process");
        EgaDemoClient_Download my_threads[] = new EgaDemoClient_Download[numThreads];
        int count = 0, savecount = 0, size = tickets.length;

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
                        } else { // try again!
                            String res = my_threads[i].getResult();
                            if (!res.toUpperCase().endsWith("SKIP")) {
                                System.out.println("Re-Try: " + _ticket + " : ");
                                my_threads[i] = new EgaDemoClient_Download(_ticket, i, size, this.api, dev_null, _name);
                                if (my_threads[i] != null) {
                                    my_threads[i].start(); // Start the download thread
                                } else {
                                    System.out.println("Error re-creating thread object for " + tickets[count]);
                                    savecount++; // skip; count as 'completed' thread, so that process can complete
                                    indices.add(i); // 'free' thread slot
                                }
                            } else {
                                savecount++; // count completed threads
                                indices.add(i); // 'free' thread slot
                            }
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
                        my_threads[index] = new EgaDemoClient_Download(tickets[count], index, size, this.api, dev_null, null);
                        
                        if (my_threads[index] != null) {
                            my_threads[index].start(); // Start the download thread
                        } else {
                            System.out.println("Error creating thread object for " + tickets[count]);
                            savecount++; // skip; count as 'completed' thread, so that process can complete
                        }
                        count++; // count started threads
                        System.out.println("Started Downloading File " + count + " of " + (size));
                    }
                }
            }

            // runs until the number of completed threads equals the number of files, and all threads completed (redundant)
        }  while ((savecount < size) || alive);
        //}  while ((savecount < size));

        System.out.println("Download Process Completed.");
    }
}
