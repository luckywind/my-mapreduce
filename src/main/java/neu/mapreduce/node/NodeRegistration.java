package neu.mapreduce.node;

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
import java.util.Iterator;

public class NodeRegistration {
    /*
     * Delete file http://jquerypluginscripts.com/nodes.txt
     * */
    public static boolean truncateRegistry() throws IOException {
        String result = sendRequest("trunc", "");
        if (result.equals("true"))
            return true;
        return false;
    }

    /*
     * Register called to server with metadata sent in 'data'
     * */
    public static String register(String data) throws IOException {
        return sendRequest("regme", data);
    }

    /*
     * Send POST request to Webservice.
     *
     * op - Type of operation to be performed as below
     *    - "trunc": Delete file http://jquerypluginscripts.com/nodes.txt
     *               Returns boolean to indicate success of failure
     *    - "regme": Register self in nodes.txt. Public IP of current caller will
     *               automatically be detected and written in the file
     * data - Actual data written in the file
     *
     * Returns - Actual string written into the nodes.txt or NODATAWRITTEN, true/false
     *           or OPNotDefined depending upon operation success (Refer PHP file
     *           for details)
     * */
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
    public static void main(String[] args) throws Exception {
        // See http://jquerypluginscripts.com/nodes.txt in the browser for result
       // registerThisNode("|slave");
//        ArrayList<NodeDAO> x = getAllNodes();
//        Iterator<NodeDAO> it = x.iterator();
//        while (it.hasNext()){
//            NodeDAO n = it.next();
//            System.out.println(n.getIp() + " " + n.getMessagingServicePort() + " " + n.getFileTransferPort());
//        }
       // System.out.println(sendRequest("regme", "data"));

        // Below line will delete nodes.txt on server
         System.out.println(truncateRegistry());
//
       // NodeDAO.releaseSocket();
    }
}