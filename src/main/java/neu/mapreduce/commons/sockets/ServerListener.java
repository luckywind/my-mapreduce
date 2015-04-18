package neu.mapreduce.commons.sockets;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Amitash on 4/13/15.
 */
public class ServerListener {
    int port;
    public static ConnectionTypes status;
    public static ConnectionTypes jobcomplete = ConnectionTypes.BUSY;
    String initMessage = null;
    boolean runJob = false;


    public ServerListener(int port){
        this.port = port;
        this.status = ConnectionTypes.IDLE;
    }

    public void startListening() throws IOException {
        System.out.println("Listening...");
        ServerSocket listener = new ServerSocket(port);
        while(true){
            Socket socket = listener.accept();
            String inputMessage;
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            while((inputMessage = in.readLine()) == null) {
                //idle the machine
            }

            //Send idle/busy response. Switch case to handle all possible requests.
            handleRequest(inputMessage, socket);
        }
    }


    private void handleRequest(String inputMessage, Socket socket) throws IOException {
        if(inputMessage.equals("status")){
            statusRequestHandler(socket);
        } else if(inputMessage.equals("runJob")){
            runJobRequestHandler(socket);
        }
    }

    private void runJobRequestHandler(Socket socket) throws IOException {

        FileOutputStream fos = null;
        ServerSocket listener = new ServerSocket(6060);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println("readyForJob");
        Socket sender = listener.accept();
        try {
            fos = new FileOutputStream("/Users/Amitash/Desktop/my-mapreduce/src/main/java/neu/mapreduce/commons/sockets/inputFile");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        InputStream in = sender.getInputStream();
        IOUtils.copy(in, fos);
        fos.close();
        sender.close();
    }

    private void statusRequestHandler(Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        if(this.status == ConnectionTypes.IDLE){
            out.println("Idle");
        }
        else if(this.status == ConnectionTypes.JOB_COMPLETE){
            //handle what happens after job is done.
            out.println("OutputSent");
        }
            out.println("Busy");
    }



    public static void main(String[] args) throws IOException {
        ServerListener listener = new ServerListener(8087);
        listener.startListening();

    }
}
