package neu.mapreduce.api.mapper;

import mapper.MyContext;
import mapper.MyMapperAPI;
import neu.mapreduce.api.shuffle.Shuffle;

import java.io.*;

/*class Etc implements MyMapperAPI {
    public String name;

    Etc() {
        name = "etc";
    }

    @Override
    public int map(String line) {
        System.out.println("inside etc map");
        return -1;
    }
}*/

/**
 * Created by mit on 4/2/15.
 */


/*interface MyMapperApi {
    String map();

}*/

/*
class ClientMapper implements MyMapperAPI {
    public String name;

    ClientMapper() {
        name = "bar";
    }

    @Override
    public int map() {
        System.out.println("inside etc map");
        return 2;
    }
}
*/

 final class Foo<T extends MyMapperAPI> {

    private Class<T> typeArgumentClass;

    public Foo(Class<T> typeArgumentClass) {

        this.typeArgumentClass = typeArgumentClass;
    }

    public T doSomethingThatRequiresNewT() throws Exception {

        T myNewT = typeArgumentClass.newInstance();

        return myNewT;
    }
}

public class FooImpl {

    public static void main(String[] args) throws Exception {
        
        //TODO:THESE SHOULD BE INPUT PARAMETER
        //TODO:FROM CLIENT
        String inputFilePath = "/home/mit/Desktop/purchase100.txt";
        String mapperClassname = "mapperImpl.AirlineMapper";

        //TODO: NEED TO GENERATE ON FLY
        String outputFilePath = "/home/mit/Desktop/map-op-1.txt";

        /*SHUFFLE*/
        Shuffle shuffle = new Shuffle();
        
        Class clientClass = Class.forName(mapperClassname);
        Foo barFoo = new Foo(clientClass);


        BufferedReader br = new BufferedReader(new FileReader(new File(inputFilePath)));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFilePath)));
        MyContext myContext = new MyContext(bw);
        
        String line;
        while((line = br.readLine())!=null){

                (barFoo.doSomethingThatRequiresNewT()).map(line, myContext);

        }
        
        bw.flush();
        br.close();
        bw.close();
        
        String locShuffleFiles = "/home/mit/Desktop/shuffle";
        shuffle.shuffle(outputFilePath, locShuffleFiles);
        //System.out.println((barFoo.doSomethingThatRequiresNewT()String locShuffleFiles = "/home/mit/Desktop/shuffle";).map());
        //System.out.println(((Etc) etcFoo.doSomethingThatRequiresNewT()).map());
    }
}