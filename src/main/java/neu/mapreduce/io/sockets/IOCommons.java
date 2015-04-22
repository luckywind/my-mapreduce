package neu.mapreduce.io.sockets;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by srikar on 4/19/15.
 */
public class IOCommons {
    public static void sendFile(String fileName, String destIP, int destPort) throws IOException {
        Socket fileSender = new Socket(destIP, destPort);
        OutputStream os = fileSender.getOutputStream();
        InputStream is = new FileInputStream(fileName);
        IOUtils.copy(is, os);
        os.close();
        is.close();
        fileSender.close();
    }


    public static void receiveFile(ServerSocket listener, String outputFileName) throws IOException {
        Socket sender = listener.accept();
        InputStream in = sender.getInputStream();
        FileOutputStream fos = new FileOutputStream(outputFileName);
        IOUtils.copy(in, fos);
        fos.close();
        in.close();
        sender.close();
    }

    public static void shutDownBufferedReader(BufferedReader bufferedReader) {
        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void shutDownBufferedWriter(BufferedWriter bufferedWriter) {
        if (bufferedWriter != null) {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }}
