package soya.framework.ecommerce.setgo.resource;


import soya.framework.ecommerce.setgo.domain.Entity;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.core.Response;

public interface DomainService<EN extends Entity> {

    @POST
    Response create(EN entity);

    @GET
    Response get(String id);

    @PUT
    Response update(EN entity);

    @DELETE
    Response delete(String id);

    @GET
    Response all();




}
