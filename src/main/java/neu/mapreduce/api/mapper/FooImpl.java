package neu.mapreduce.api.mapper;

import mapper.MyMapperAPI;

import neu.mapreduce.client.client;

/**
 * Created by mit on 4/2/15.
 */


/*interface MyMapperApi {
    String map();

}*/

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

class Etc implements MyMapperAPI {
    public String name;

    Etc() {
        name = "etc";
    }

    @Override
    public int map() {
        System.out.println("inside etc map");
        return -1;
    }
}

 final class Foo<T extends MyMapperAPI> {

    private Class<T> typeArgumentClass;

    public Foo(Class<T> typeArgumentClass) {

        this.typeArgumentClass = typeArgumentClass;
    }

    public T doSomethingThatRequiresNewT() throws Exception {

        T myNewT = typeArgumentClass.newInstance();

        return myNewT;
    }
     
    public void someMethod() throws Exception {
        T barFoo = doSomethingThatRequiresNewT();
        barFoo.map();
        
    } 
}

public class FooImpl {

    public static void main(String[] args) throws Exception {
        String string = "mapperImpl.AirlineMapper";
        Class clientClass = Class.forName(string);
        Foo barFoo = new Foo(clientClass);
        Foo<Etc> etcFoo = new Foo<Etc>(Etc.class);

        System.out.println((barFoo.doSomethingThatRequiresNewT()).map());
        System.out.println(((Etc) etcFoo.doSomethingThatRequiresNewT()).map());
    }
    
}

