package com.github.ftrossbach.kiqr.client.service.rest;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by ftr on 10/03/2017.
 */
@FunctionalInterface
public interface URISupplier<T> {


    T get() throws URISyntaxException, IOException;
}
