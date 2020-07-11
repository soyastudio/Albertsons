package soya.framework.ecommerce.setgo.domain.support;

import soya.framework.ecommerce.setgo.domain.Aggregate;
import soya.framework.ecommerce.setgo.domain.BoundedContext;
import soya.framework.ecommerce.setgo.domain.Entity;

public class DefaultBoundedContext<E extends Entity>  implements BoundedContext<E> {
    private Aggregate<E> aggregate;

    @Override
    public Aggregate<E> getAggregate() {
        return aggregate;
    }

    @Override
    public <T extends Entity> T[] listObjectValues(Class<T> type) {
        return null;
    }

    @Override
    public <T extends Entity> T getObjectValue(Object id, Class<T> type) {
        return null;
    }

    @Override
    public <T extends Entity> void addObjectValue(T t) {

    }

    @Override
    public <T extends Entity> void updateObjectValue(T t) {

    }

    @Override
    public <T extends Entity> void removeObjectValue(Object id, Class<T> type) {

    }

    @Override
    public Object create(E entity) {
        return null;
    }

    @Override
    public E get(String id) {
        return null;
    }

    @Override
    public void update(E entity) {

    }

    @Override
    public void delete(String id) {

    }

    @Override
    public <T extends Entity> Object addChild(T child, Object entityId) {
        return null;
    }

    @Override
    public <T extends Entity> Object getChild(Object childId, Object entityId, Class<T> childType) {
        return null;
    }

    @Override
    public <T extends Entity> void updateChild(T child) {

    }

    @Override
    public <T extends Entity> void deleteChild(T child) {

    }

    @Override
    public <T extends Entity> T[] listChildren(Object entityId, Class<T> childType) {
        return null;
    }

    @Override
    public <T extends Entity> void addChildren(T[] children, Object entityId) {

    }
}
