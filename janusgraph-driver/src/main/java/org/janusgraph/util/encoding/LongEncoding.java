// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.util.encoding;

/**
 * Utility class for encoding longs in strings based on:
 * See <a href="https://stackoverflow.com/questions/2938482/encode-decode-a-long-to-a-string-using-a-fixed-set-of-letters-in-java">stackoverflow</a>
 *
 * @author https://stackoverflow.com/users/276101/polygenelubricants
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LongEncoding {

    public static String decode(String s) {
        return s;
    }

    public static String encode(String num) {
        return num;
    }
}
