package com.github.ftrossbach.kiqr.client.service.rest;

import java.io.IOException;

/**
 * Created by ftr on 10/03/2017.
 */
public interface MappingFunction<T,U> {

    U apply(T t) throws IOException;

}
