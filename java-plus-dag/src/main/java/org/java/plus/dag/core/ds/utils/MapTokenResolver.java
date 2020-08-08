package org.java.plus.dag.core.ds.utils;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MapTokenResolver implements TokenResolver {

    protected Map<String, Object> tokenMap;

    @Setter
    @Getter
    public Set<String> tokenSet;

    public MapTokenResolver() {
        tokenSet = new HashSet<>();
    }

    public MapTokenResolver(Map<String, Object> tokenMap) {
        this.tokenMap = tokenMap;
        this.tokenSet = new HashSet<>();
    }

    @Override
    public String resolveToken(String tokenName) {
        tokenSet.add(tokenName);
        Object token = tokenMap.get(tokenName);
        if (!Objects.isNull(token)) {
            return token.toString();
        }
        return null;
    }
}