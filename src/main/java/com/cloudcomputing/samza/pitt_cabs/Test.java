package com.cloudcomputing.samza.pitt_cabs;

/**
 * @author Siqi Wang siqiw1 on 4/20/16.
 */
public class Test {


    public static void main(String[] args) {
        String[] test = new String[3];
        test[0] = "a";
        test[1] = "b";
        test[2] = "c";
        String output = test[0];
        for (int i = 1; i < 3; i++) {
            output += ":" + test[i];
        }
        System.out.println(output);
    }
}
