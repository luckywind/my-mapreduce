package neu.mapreduce.commons.sockets;

/**
 * Created by Amitash on 4/13/15.
 */
public class ThreadTest extends Thread {
    int a;
    public void run(){
        for(int i = 0; i < 10000000; i++){
          a = i;
        }
        ServerListener.status = ConnectionTypes.IDLE;
        ServerListener.jobcomplete = ConnectionTypes.IDLE;
    }
}
