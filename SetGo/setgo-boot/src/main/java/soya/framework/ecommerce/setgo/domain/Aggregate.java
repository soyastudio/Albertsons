package soya.framework.ecommerce.setgo.domain;

public interface Aggregate<E extends Entity> {
    String getName();

    Class<E> getRootEntity();

    String[] getValueObjectNames();

    Class<? extends Entity>[] getValueObjectEntities();

    Class<? extends Entity>[] getDependentEntities();

    Class<? extends Entity> getEntity(String name);

    String getSchema();

    String getSchema(String entityName);
}
