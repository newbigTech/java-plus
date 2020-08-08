package org.java.plus.dag.core.ds.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class KeyPair {

    private String pkey;
    private List<String> skeys;

    public static KeyPair from(String pkey) {
        return new KeyPair(pkey, new ArrayList<>());
    }

    public static KeyPair from(List<String> skeys) {
        return new KeyPair("", skeys);
    }
}
