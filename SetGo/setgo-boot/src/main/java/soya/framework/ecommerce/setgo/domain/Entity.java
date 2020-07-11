package soya.framework.ecommerce.setgo.domain;

public interface Entity<T> {
    Class<T> getIdentityType();

    T getId();

    String getName();

    String getDescription();
}
