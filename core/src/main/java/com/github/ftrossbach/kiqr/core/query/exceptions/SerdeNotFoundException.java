package com.github.ftrossbach.kiqr.core.query.exceptions;

/**
 * Created by ftr on 03/03/2017.
 */
public class SerdeNotFoundException extends RuntimeException{

    public SerdeNotFoundException() {
    }

    public SerdeNotFoundException(String message) {
        super(message);
    }

    public SerdeNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerdeNotFoundException(Throwable cause) {
        super(cause);
    }

    public SerdeNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
