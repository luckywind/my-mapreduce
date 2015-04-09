package neu.mapreduce.api.factory;

import api.MyMapperAPI;
import api.MyWriteComparable;

/**
 * Created by mit,srikar,vishal on 4/8/15.
 */


public final class WriteComparableFactory<T extends MyWriteComparable> {

    private Class<T> typeArgumentClass;
    private T singletonObject;
    public WriteComparableFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {

        this.typeArgumentClass = typeArgumentClass;
        this.singletonObject = typeArgumentClass.newInstance();
    }

    public T getSingletonObject() {
        return singletonObject;
    }

    public T getNewInstance() throws Exception {

        T myNewT = typeArgumentClass.newInstance();

        return myNewT;
    }
}