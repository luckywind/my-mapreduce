package neu.mapreduce.autodiscovery;

/**
 * Created by Vishal on 4/22/15.
 */

import org.apache.commons.io.IOUtils;

import java.io.InputStreamReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;

public class NodeRegistration {


    /**
     * Delete registry file http://jquerypluginscripts.com/nodes.txt
     * @return true if the file successfully deleted
     * @throws IOException
     */
     public static boolean truncateRegistry() throws IOException {
        String result = sendRequest("trunc", "");
        if (result.equals("true"))
            return true;
        return false;
    }

    /**
     * Register called to server with metadata sent in 'data'
      * @param Metadata about this autodiscovery
     * @return Returns the output received from web service
     * @throws IOException
     */
     public static String register(String data) throws IOException {
        return sendRequest("regme", data);
    }




    /**
     * Send POST request to Webservice.
      * @param op Type of operation to be performed as below
                    "trunc": Delete file http://jquerypluginscripts.com/nodes.txt
                             Returns boolean to indicate success of failure
                    "regme": Register self in nodes.txt. Public IP of current caller will
                             automatically be detected and written in the file
     * @param data Actual data written in the file
     * @return Actual string written into the nodes.txt or NODATAWRITTEN,
     *         true/false or OPNotDefined depending upon operation success (Refer PHP file for details)
     * @throws IOException
     */
     public static String sendRequest(String op, String data) throws IOException {
        URL url = new URL("http://www.jquerypluginscripts.com/mr.php");
        URLConnection conn = url.openConnection();
        conn.setDoOutput(true);
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());

        writer.write("op=" + op + "&data=" + data);
        writer.flush();
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        writer.close();
        reader.close();
        return new String(sb);
    }

    /**
     * Get all slave nodes in the registry
     * @return Array list having all the slave nodes.
     * @throws IOException
     */
    public static ArrayList<NodeDAO> getAllNodes() throws IOException {

        InputStream input = new URL( "http://jquerypluginscripts.com/nodes.txt" ).openStream();
        ArrayList<NodeDAO> al = new ArrayList<>();
        ArrayList<String> allNodesIP = new ArrayList<>();
        try {
            String[] allNodesContent = IOUtils.toString(input).split("\n");
            for(String eachNode: allNodesContent) {
                String[] ipList = eachNode.split("\\|");
                al.add(new NodeDAO(ipList[0]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(input);
        }
        return  al;
    }

    /**
     * Get various IP addresses of the current slave autodiscovery. These IPs are taken Operating System
     * @return All IPs representing this slave in string
     * @throws SocketException
     */
    public static String getIPsInString() throws SocketException {
        StringBuilder sb = new StringBuilder();
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while(e.hasMoreElements())
        {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements())
            {
                InetAddress i = (InetAddress) ee.nextElement();
                sb.append(i.getHostAddress()+" ");
            }
        }
        return new String(sb);
    }
}