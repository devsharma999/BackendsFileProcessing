package com.intern.batch.exception;

public class InvalidHeaderException extends CustomBatchException {
    public InvalidHeaderException(String filename) {
        super("Invalid or missing headers in file: " + filename);
    }
}
