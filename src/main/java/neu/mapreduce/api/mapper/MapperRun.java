package neu.mapreduce.api.mapper;


import mapper.MyMapperAPI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by mit on 4/2/15.
 * *
 */
public class MapperRun<T extends MyMapperAPI> {
    /*
    @mitmehta
    Should have the location of jar and location where input split is stored(on local)
     */
    private Class<T> mapperRunInstance;
    
    public MapperRun(Class<T> mapperRunInstance) {
        this.mapperRunInstance = mapperRunInstance;
    }
    
    public T getNewMapperRun() throws IllegalAccessException, InstantiationException {
        return mapperRunInstance.newInstance();
    }
    
    public static void main(String[] args) throws IOException {
        /*String jarname = "client-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String pathjar = "/home/mit/Desktop/input/api/target/";*/
        
//        Foo<Bar> barFoo = new Foo<Bar>(Bar.class);
        //MapperRun<T> clientMapper = new MapperRun<T>();
        Runtime re = Runtime.getRuntime();
        BufferedReader reader;
        try{
            String cmd = "java -jar /home/mit/Desktop/input/client/target/client-1.0-SNAPSHOT-jar-with-dependencies.jar";
            Process process = re.exec(cmd);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String result;
            while((result = reader.readLine())!=null){
                System.out.println(result);
            }
        }catch (IOException e) {

        }

    }
}
