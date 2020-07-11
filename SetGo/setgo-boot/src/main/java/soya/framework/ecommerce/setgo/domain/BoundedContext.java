package soya.framework.ecommerce.setgo.domain;

public interface BoundedContext<E extends Entity> {
    Aggregate<E> getAggregate();

    //---------------- Value Objects:
    <T extends Entity> T[] listObjectValues(Class<T> type);

    <T extends Entity> T getObjectValue(Object id, Class<T> type);

    <T extends Entity> void addObjectValue(T t);

    <T extends Entity> void updateObjectValue(T t);

    <T extends Entity> void removeObjectValue(Object id, Class<T> type);

    //---------------- Root Entity
    Object create(E entity);

    E get(String id);

    void update(E entity);

    void delete(String id);

    //---------------- Dependent Entities
    <T extends Entity> Object addChild(T child, Object entityId);

    <T extends Entity> Object getChild(Object childId, Object entityId, Class<T> childType);

    <T extends Entity> void updateChild(T child);

    <T extends Entity> void deleteChild(T child);

    <T extends Entity> T[] listChildren(Object entityId, Class<T> childType);

    <T extends Entity> void addChildren(T[] children, Object entityId);

}
