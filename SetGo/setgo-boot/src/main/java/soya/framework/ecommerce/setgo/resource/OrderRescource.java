package soya.framework.ecommerce.setgo.resource;

import io.swagger.annotations.Api;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Component
@Path("/order")
@Api(value = "Order Service")
public class OrderRescource {

    @GET
    @Path("/current")
    public Response index() {
        return Response.ok().build();
    }
}
