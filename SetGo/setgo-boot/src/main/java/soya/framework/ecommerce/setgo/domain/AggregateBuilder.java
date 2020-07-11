package soya.framework.ecommerce.setgo.domain;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class AggregateBuilder {

    private String name;
    private Class<? extends Entity> root;
    private Map<String, Class<? extends Entity>> valueObjects = new LinkedHashMap<>();
    private Map<String, Class<? extends Entity>> dependencies = new LinkedHashMap<>();

    private AggregateBuilder() {
    }

    public AggregateBuilder name(String name) {
        this.name = name;
        return this;
    }

    public AggregateBuilder rootEntity(Class<? extends Entity> root) {
        this.root = root;
        return this;
    }

    public AggregateBuilder addValueObject(String name, Class<? extends Entity> valueObject) {
        if (!valueObjects.containsKey(name) && !dependencies.containsKey(name)) {
            valueObjects.put(name, valueObject);
        } else {
            throw new IllegalArgumentException("Entity name already exists: " + name);
        }

        return this;
    }

    public AggregateBuilder addDependentEntity(String name, Class<? extends Entity> dependentEntity) {
        if (!valueObjects.containsKey(name) && !dependencies.containsKey(name)) {
            dependencies.put(name, dependentEntity);
        } else {
            throw new IllegalArgumentException("Entity name already exists: " + name);
        }
        return this;
    }

    public AggregateBuilder remove(String name) {
        if (valueObjects.containsKey(name)) {
            valueObjects.remove(name);
        } else if ((dependencies.containsKey(name))) {
            dependencies.remove(name);
        }
        return this;
    }

    public Aggregate build() {
        return new DefaultAggregate(name, root, valueObjects, dependencies);
    }

    public static AggregateBuilder builder() {
        return new AggregateBuilder();
    }

    public static AggregateBuilder builder(Aggregate aggregate) {
        AggregateBuilder builder = new AggregateBuilder();
        builder.name(aggregate.getName());
        builder.rootEntity(aggregate.getRootEntity());

        for (String vo : aggregate.getValueObjectNames()) {
            builder.valueObjects.put(vo, aggregate.getEntity(vo));
        }

        for (String dep : aggregate.getDependentEntityNames()) {
            builder.dependencies.put(dep, aggregate.getEntity(dep));
        }

        return builder;
    }

    static class DefaultAggregate implements Aggregate {
        private final String name;
        private final Class<? extends Entity> root;
        private final Map<String, Class<? extends Entity>> valueObjects;
        private final Map<String, Class<? extends Entity>> dependencies;

        private final String[] valueObjectNames;
        private final String[] dependentEntityNames;

        private DefaultAggregate(String name, Class<? extends Entity> root, Map<String, Class<? extends Entity>> valueObjects, Map<String, Class<? extends Entity>> dependencies) {
            this.name = name;
            this.root = root;
            this.valueObjects = ImmutableMap.copyOf(valueObjects);
            this.dependencies = ImmutableMap.copyOf(dependencies);

            this.valueObjectNames = new ArrayList<String>(valueObjects.keySet()).toArray(new String[valueObjects.size()]);
            this.dependentEntityNames = new ArrayList<String>(dependencies.keySet()).toArray(new String[dependencies.size()]);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Class getRootEntity() {
            return root;
        }

        @Override
        public String[] getValueObjectNames() {
            return valueObjectNames;
        }

        @Override
        public Class<? extends Entity>[] getValueObjectEntities() {
            return (Class<? extends Entity>[]) new ArrayList<Class<? extends Entity>>(valueObjects.values()).toArray(new Class<?>[valueObjects.size()]);
        }

        @Override
        public String[] getDependentEntityNames() {
            return dependentEntityNames;
        }

        @Override
        public Class<? extends Entity>[] getDependentEntities() {
            return (Class<? extends Entity>[]) new ArrayList<Class<? extends Entity>>(dependencies.values()).toArray(new Class<?>[dependencies.size()]);
        }

        @Override
        public Class<? extends Entity> getEntity(String name) {
            if (valueObjects.containsKey(name)) {
                return valueObjects.get(name);
            } else if (dependencies.containsKey(name)) {
                return dependencies.get(name);
            } else {
                return null;
            }
        }

        @Override
        public String getSchema() {
            return null;
        }

        @Override
        public String getSchema(String entityName) {
            return null;
        }
    }
}
