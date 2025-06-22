package com.intern.batch.exception;

public class UnsupportedFileFormatException extends CustomBatchException {
    public UnsupportedFileFormatException(String filename) {
        super("Unsupported file format for file: " + filename);
    }
}


