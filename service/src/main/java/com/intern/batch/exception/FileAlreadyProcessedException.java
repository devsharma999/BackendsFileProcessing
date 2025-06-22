package com.intern.batch.exception;

public class FileAlreadyProcessedException extends CustomBatchException {

    public FileAlreadyProcessedException(String message) {
        super(message);
    }
}