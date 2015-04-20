package neu.mapreduce.core.factory;

import api.MyWriteComparable;

/**
 * Created by mit,srikar,vishal on 4/8/15.
 */

/**
 * Factory which creates {@link api.MyWriteComparable} subclass instances
 * at runtime. It creates instance only once and returns same instance
 * if called again.
 *
 * @param <T> creates instances of class T which extends {@link api.MyMapper}
 */

public final class WriteComparableFactory<T extends MyWriteComparable> {

    private Class<T> typeArgumentClass;
    private T singletonObject;

    /**
     * Constructor which creates instance using class object
     *
     * @param typeArgumentClass Class whose instance need to be created
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public WriteComparableFactory(Class<T> typeArgumentClass) throws IllegalAccessException, InstantiationException {
        this.typeArgumentClass = typeArgumentClass;
        this.singletonObject = typeArgumentClass.newInstance();
    }

    /**
     * @return singleton object
     */
    public T getSingletonObject() {
        return singletonObject;
    }

    /**
     * Creates new instance of the given class
     *
     * @return a new instance of given class
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public T getNewInstance() throws IllegalAccessException, InstantiationException {
        T myNewT = typeArgumentClass.newInstance();
        return myNewT;
    }

}