package com.parul.bunching.detector;

/** A position that can never be processed, so retrying it is pointless. Sent straight to the DLT. */
public class InvalidVehicleEventException extends RuntimeException {

    public InvalidVehicleEventException(String message) {
        super(message);
    }
}
