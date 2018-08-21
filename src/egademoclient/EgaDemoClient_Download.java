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

import java.io.File;
import java.util.concurrent.Callable;
import uk.ac.embl.ebi.ega.egadbapiwrapper.EgaDBAPIWrapper;

/**
 *
 * @author asenf
 * 
 * Takes care of downloading one File, including MD5 verification, etc. by calling
 * the API function for downloading a file. The purpose of this class is to allow
 * multiple downloads to be initiated and submitted to an Executor service, for 
 * efficient parallel download of large dataset requests.
 */
public class EgaDemoClient_Download extends Thread implements Callable {

    private final String ticket, down_name, org;
    private final int index, tot;
    private final EgaDBAPIWrapper api;
    private final boolean dev_null;
    
    private String result;
    private boolean success = false;
    
    public EgaDemoClient_Download(String ticket, int index, int tot, EgaDBAPIWrapper api, boolean dev_null, String down_name) {
        if (ticket.contains("?org=")) {
            this.org = ticket.substring(ticket.indexOf("?org=")+5);
            this.ticket = ticket.substring(0, ticket.indexOf("?org="));
        } else {
            this.ticket = ticket;
            this.org = "";
        }
        this.index = index;
        this.tot = tot;
        this.api = api;
        this.dev_null = dev_null;
        this.down_name = down_name;
        
        
        //this.api.setVerbose(true);
        
        this.result = "";
    }
    
    @Override
    public Object call() {
        String down_name_ = this.down_name;
        if (down_name_ != null && down_name_.endsWith("gpg")) {
            if (this.org.length()==0)
                down_name_ = down_name_.substring(0, down_name_.length()-3) + "cip";
        } else if (down_name_ != null && down_name_.endsWith("cip")) {
            if (this.org.length()>0)
                down_name_ = down_name_.substring(0, down_name_.length()-3) + "gpg";
        }
        if (down_name_ != null)
            down_name_ = down_name_.replaceAll("/", "_");
        
        System.out.println("Starting download: " + this.down_name + "  (" + this.index + "/" + this.tot + ")");
        
        long time = System.currentTimeMillis();
        this.success = false;
        //String[] ds = this.dev_null?this.api.download_ticket_null_url(ticket):this.api.download_ticket_url(ticket, down_name_);
        String[] ds = this.api.download(ticket, down_name_, org);
        time = System.currentTimeMillis() - time;
        long length = 0;
        if (ds!=null && ds.length > 0) {
            if (ds[0].equalsIgnoreCase("Ticket is Locked!"))
                length = -10;
            else
                length = this.dev_null?Long.parseLong(ds[0]):(new File(ds[0])).length();
        }
        
        this.success = (length > 0 || (ds!=null && ds.length>1 && ds[1].equalsIgnoreCase("Success")));
        if (success) {
            double rate = (length * 1.0 / 1024.0 / 1024.0) / (time * 1.0 / 1000.0);

            StringBuilder sb = new StringBuilder();
            try {
                sb.append("Completed Download: ").append(this.down_name).append("\n");
                sb.append("Completed Download Target: ");
                for (int i=0; i<ds.length; i++)
                    sb.append("  ").append(ds[i].substring(ds[i].lastIndexOf("/")+1)).append(" ");
                sb.append("\n");
                sb.append("Rate: ").append(rate).append(" MB/s").append("\n");
            } catch (Throwable t) {
                sb.append("Error requesting ").append(this.down_name).append(" (").append(this.ticket).append(")").append("\n");
            }
            this.result = sb.toString();
        } else {
            this.result = "Download Failed: " + this.down_name + "  " + this.ticket;
            if ( (length==-10) || ((ds!=null && ds.length > 2 && ds[2].equalsIgnoreCase("true")))) {
                System.out.println("Skipping this ticket!");
                this.result += "   SKIP";
            }
        }

        return this.result;
    }
    
    @Override
    public void run() {
        call();
    }
    
    public int getIndex() {
        return this.index;
    }

    public String getResult() {
        return this.result;
    }
    
    public boolean getSuccess() {
        return this.success;
    }
    
    public String getTicket() {
        return this.ticket;
    }
    
    public String getDownName() {
        return this.down_name;
    }
}
