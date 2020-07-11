package soya.framework.ecommerce.setgo.configuration;

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import io.swagger.models.Swagger;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.annotation.Configuration;
import soya.framework.ecommerce.setgo.resource.DomainService;

import javax.ws.rs.ApplicationPath;

@Configuration
@ApplicationPath("/api")
public class JerseyConfig extends ResourceConfig {

    public JerseyConfig() {
        System.out.println("========================= !!!");

        register(GsonMessageBodyHandler.class);

        packages(DomainService.class.getPackage().getName());

        swaggerConfig();
    }

    private Swagger swaggerConfig() {
        this.register(ApiListingResource.class);
        this.register(SwaggerSerializers.class);

        BeanConfig swaggerConfigBean = new BeanConfig();
        swaggerConfigBean.setConfigId("SetGo");
        swaggerConfigBean.setTitle("SetGo");
        //swaggerConfigBean.setVersion("v1");
        swaggerConfigBean.setContact("wenqun.soya@gmail.com");
        swaggerConfigBean.setSchemes(new String[]{"http"});
        swaggerConfigBean.setBasePath("/api");
        swaggerConfigBean.setResourcePackage(DomainService.class.getPackage().getName());
        swaggerConfigBean.setPrettyPrint(true);
        swaggerConfigBean.setScan(true);

        return swaggerConfigBean.getSwagger();
    }
}
