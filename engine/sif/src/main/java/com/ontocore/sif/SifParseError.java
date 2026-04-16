package com.ontocore.sif;

/**
 * Raised when a raw JSON map cannot be parsed into a {@link SifOperation}.
 */
public class SifParseError extends RuntimeException {
    public SifParseError(String message) {
        super(message);
    }
}
