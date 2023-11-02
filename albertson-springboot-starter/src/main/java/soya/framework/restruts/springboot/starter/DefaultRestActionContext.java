package soya.framework.restruts.springboot.starter;

import soya.framework.restruts.*;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

class DefaultRestActionContext implements RestActionContext {
    private Map<ActionMapping.Path, ActionMapping> registrations = new HashMap();
    private DependencyInjector injector;
    private Map<String, Serializer> serializerMap = new HashMap<>();
    private RestActionFactory actionFactory = new DefaultRestActionFactory();

    private String apiPath;
    private String api;
    public DependencyInjector getDependencyInjector() {
        return injector;
    }

    public RestActionFactory getActionFactory() {
        return actionFactory;
    }

    public Serializer getSerializer(String mediaType) {
        String key = mediaType.toUpperCase();
        if (serializerMap.containsKey(key)) {
            return serializerMap.get(key);
        } else {
            return new DefaultSerializer();
        }
    }

    @Override
    public ActionMapping[] getActionMappings() {
        List<ActionMapping> list = new ArrayList<>(registrations.values());
        Collections.sort(list);
        return list.toArray(new ActionMapping[list.size()]);
    }

    public ActionMapping getActionMapping(HttpServletRequest request) {
        return registrations.get(ActionMapping.path(request.getMethod(), request.getPathInfo()));
    }

    @Override
    public String getApiPath() {
        return apiPath;
    }

    public void setApiPath(String apiPath) {
        this.apiPath = apiPath;
    }

    public String getApi() {
        return api;
    }

    public void setApi(String api) {
        this.api = api;
    }

    DefaultRestActionContext register(RestActionFactory actionFactory) {
        this.actionFactory = actionFactory;
        return this;
    }

    DefaultRestActionContext register(DependencyInjector injector) {
        this.injector = injector;
        return this;
    }

    DefaultRestActionContext register(String mediaType, Serializer serializer) {
        serializerMap.put(mediaType.toUpperCase(), serializer);
        return this;
    }

    DefaultRestActionContext register(RestActionLoader loader) {
        loader.load().forEach(e -> {
            this.register(e);
        });
        return this;
    }

    DefaultRestActionContext register(ActionMapping mapping) {
        registrations.put(mapping.getPath(), mapping);
        return this;
    }

    static class DefaultSerializer implements Serializer {
        @Override
        public <T> byte[] serialize(T o) throws SerializationException {
            return o.toString().getBytes();
        }

        @Override
        public <T> T deserialize(byte[] data, Class<T> type) throws SerializationException {
            return (T) new String(data);
        }
    }

}
