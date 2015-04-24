package neu.mapreduce.io.sockets;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Mit, Srikar, Vishal, Amitash on 4/19/15.
 */
public class IOCommons {
    /**
     * To send file to given IP and port*
     *
     * @param fileName Path of file name to send
     * @param destIP   IP address of destination
     * @param destPort Port Number of destination
     * @throws IOException
     */
    public static void sendFile(String fileName, String destIP, int destPort) throws IOException {
        Socket fileSender = new Socket(destIP, destPort);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(fileName);
        IOUtils.copy(is, os);
        os.close();
        is.close();
        fileSender.close();
    }


    /**
     * To receive file given listner socket *
     *
     * @param listener       ServerSocket of the listener
     * @param outputFileName Name of the output file
     * @throws IOException
     */
    public static void receiveFile(ServerSocket listener, String outputFileName) throws IOException {
        Socket sender = listener.accept();
        InputStream in = sender.getInputStream();
        FileOutputStream fos = new FileOutputStream(outputFileName);
        IOUtils.copy(in, fos);
        fos.close();
        in.close();
        sender.close();
    }

    /**
     * To close BufferedReader *
     *
     * @param bufferedReader BufferedReader to close
     */
    public static void shutDownBufferedReader(BufferedReader bufferedReader) {
        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * To close BufferedWriter *
     *
     * @param bufferedWriter BufferedWriter to close
     */
    public static void shutDownBufferedWriter(BufferedWriter bufferedWriter) {
        if (bufferedWriter != null) {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * To close ServerSocket*
     *
     * @param serverSocket ServerSocket to close
     */
    public static void shutDownServerSocket(ServerSocket serverSocket) {
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (final IOException e) {
                // can never happen
            }
        }
    }

    /**
     * To close DatagramSocket*
     *
     * @param dataSocket DatagramSocket to close
     */
    public static void shutDownDatagramSocket(DatagramSocket dataSocket) {
        if (dataSocket != null) {
            dataSocket.close();
        }
    }
}
