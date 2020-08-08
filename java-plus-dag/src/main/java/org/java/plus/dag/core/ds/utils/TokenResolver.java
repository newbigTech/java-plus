/*
 * Copyright 2011-2015 PrimeFaces Extensions
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * $Id$
 */

package org.java.plus.dag.core.ds.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Interface for resolving of tokens found via {@link TokenReplacingReader}.
 *
 * @author Oleg Varaksin (ovaraksin@googlemail.com)
 */
public interface TokenResolver {

    String resolveToken(String token) throws IOException;

    default void addToken(String token) throws IOException {
    }

    default Set<String> getToken() throws IOException {
        return new HashSet<>();
    }
}
