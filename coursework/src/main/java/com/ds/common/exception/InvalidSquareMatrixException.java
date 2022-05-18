package com.ds.common.exception;

public class InvalidSquareMatrixException extends Exception {
    public InvalidSquareMatrixException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
