package neu.mapreduce.core.factory;

import api.MyMapperAPI;
import api.MyReducer;

import java.io.Reader;

/**
 * Created by mit,srikar,vishal on 4/8/15.
 */


public final class ReducerFactory<T extends MyReducer>{

    private Class<T> typeArgumentClass;
    private T singletonObject;

    public ReducerFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        singletonObject = typeArgumentClass.newInstance();
    }

    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }
}