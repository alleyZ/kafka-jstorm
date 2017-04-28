package com.alleyz.kafka.example.expection;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
public class NoTopicException extends Exception{
    public NoTopicException(String message) {
        super(message);
    }
}
