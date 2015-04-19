package neu.mapreduce.commons.sockets;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Amitash on 4/18/15.
 */
public class MasterScheduler {
    ArrayList<String> fileSplits;
    String inputJar;
    HashMap<String, Socket> slaves;
    //freeSlaveID is a string in the format: ip:port. eg: 192.168.1.1:8087
    String freeSlaveID;
    String curSplit;


    public MasterScheduler(ArrayList<String> fileSplits, String inputJar, HashMap<String, Socket> slaves){
        this.fileSplits = fileSplits;
        this.inputJar = inputJar;
        this.slaves = slaves;
    }

    public void schedule() throws IOException {
        boolean isCompleted = false;
        while (!isCompleted) {
            //Do the mapper phase
            if (!(fileSplits.isEmpty())) {
                //Allocate task
                allocateMapTask();
            } else {
                //Check for mapper completion
                isCompleted = checkForCompletion();
            }

        }

        //Start reducer here
        System.out.println("Success!");


    }

    private boolean checkForCompletion() throws IOException {
        for (String slaveID : slaves.keySet()) {
            Socket slaveSocket = slaves.get(slaveID);
            PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(slaveSocket.getInputStream()));
            out.println("status");
            if (in.readLine() != "Complete") {
                return false;
            } else {
                //Handle what to do with successful mappers and the data file locations.
                out.println("changeStatus");
            }
        }
        return true;
    }


    private void allocateMapTask() throws IOException {
        //Find a free slave
        if (findFreeSlave()) {
            //Allocate job to slave
            this.curSplit = fileSplits.get(0);
            fileSplits.remove(0);
            initiateSlaveJob();
        }

    }

    private void initiateSlaveJob() throws IOException {
        Socket slaveSocket = slaves.get(this.freeSlaveID);
        PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(slaveSocket.getInputStream()));
        out.println("runJob");
        in.readLine();
        //while(!(in.readLine().equals("readyForJob"))) {
        //}
        //Send split
        Socket fileSender = new Socket(getIp(this.freeSlaveID), 6060);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(this.curSplit);
        IOUtils.copy(is, os);
        //Send jar;
        is = new FileInputStream(this.inputJar);
        IOUtils.copy(is, os);
        fileSender.close();
    }

    private String getIp(String freeSlaveID) {
        String[] slaveIdSplit = freeSlaveID.split(":");
        return slaveIdSplit[0];
    }

    private boolean findFreeSlave() throws IOException {
        for (String slaveID : slaves.keySet()) {
            Socket slaveSocket = slaves.get(slaveID);
            PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(slaveSocket.getInputStream()));
            out.println("status");
            if(in.readLine().equals("Idle")){
                this.freeSlaveID = slaveID;
                return true;
            }
        }
        return false;
    }
}

