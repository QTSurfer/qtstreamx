# Third-party licenses

This file records the third-party code distributed in the QTStreamX CLI
archives. The release automation validates the built runtime against this
inventory and includes this file in every application archive.

## Runtime distribution set

The JVM distribution contains these four external runtime JARs. GraalVM links
the same components into the native distribution.

| Maven coordinate | License | Upstream license |
|---|---|---|
| `com.fasterxml.jackson.core:jackson-annotations:2.18.9` | Apache-2.0 | https://github.com/FasterXML/jackson-annotations/blob/jackson-annotations-2.18.9/LICENSE |
| `com.fasterxml.jackson.core:jackson-core:2.18.9` | Apache-2.0 | https://github.com/FasterXML/jackson-core/blob/jackson-core-2.18.9/LICENSE |
| `com.fasterxml.jackson.core:jackson-databind:2.18.9` | Apache-2.0 | https://github.com/FasterXML/jackson-databind/blob/jackson-databind-2.18.9/LICENSE |
| `org.slf4j:slf4j-api:2.0.16` | MIT | https://github.com/qos-ch/slf4j/blob/v_2.0.16/LICENSE.txt |

The Apache License 2.0 terms are reproduced in `LICENSE`.

## Published library runtime graph

JitPack library artifacts do not bundle their dependencies, but their resolved
external runtime graph is pinned and checked against this inventory. A change
to any coordinate fails `verifyPublishedRuntimeLicenses` until reviewed.

| Maven coordinate | License | Upstream license |
|---|---|---|
| `com.fasterxml.jackson.core:jackson-annotations:2.18.9` | Apache-2.0 | https://github.com/FasterXML/jackson-annotations/blob/jackson-annotations-2.18.9/LICENSE |
| `com.fasterxml.jackson.core:jackson-core:2.18.9` | Apache-2.0 plus bundled notices below | https://github.com/FasterXML/jackson-core/blob/jackson-core-2.18.9/LICENSE |
| `com.fasterxml.jackson.core:jackson-databind:2.18.9` | Apache-2.0 | https://github.com/FasterXML/jackson-databind/blob/jackson-databind-2.18.9/LICENSE |
| `io.nats:jnats:2.20.6` | Apache-2.0 | https://github.com/nats-io/nats.java/blob/2.20.6/LICENSE |
| `org.bouncycastle:bcprov-lts8on:2.73.11` | Bouncy Castle License | https://www.bouncycastle.org/about/license/ |
| `org.java-websocket:Java-WebSocket:1.6.0` | MIT | https://github.com/TooTallNate/Java-WebSocket/blob/v1.6.0/LICENSE |
| `org.msgpack:msgpack-core:0.9.11` | Apache-2.0 | https://github.com/msgpack/msgpack-java/blob/v0.9.11/LICENSE.txt |
| `org.slf4j:slf4j-api:2.0.16` | MIT | https://github.com/qos-ch/slf4j/blob/v_2.0.16/LICENSE.txt |

## Jackson notices

Jackson is copyright 2007-, Tatu Saloranta, and its contributors.
`jackson-core` also contains FastDoubleParser, copyright 2023 Werner
Randelshofer, and Schubfach, copyright 2018-2020 Raffaello Giulietti. Those
two embedded components are licensed under the MIT License. FastDoubleParser
in turn contains code from `fast_float`, copyright 2021 the fast_float
authors, under the MIT License; a Java port of Daniel Lemire's fast_float
work under the Boost Software License 1.0; and portions of `bigint`, copyright
2022 Tim Buktu, under the BSD 2-Clause License.

- Copyright 2023 Werner Randelshofer.
- Copyright 2018-2020 Raffaello Giulietti.
- Copyright 2021 the fast_float authors.
- Copyright Daniel Lemire.
- Copyright 2022 Tim Buktu.

## MIT components

SLF4J is Copyright 2004-2022 QOS.ch Sarl (Switzerland). The other MIT
copyrights are identified in the Jackson notices above.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Boost Software License 1.0 component

Copyright Daniel Lemire.

Permission is hereby granted, free of charge, to any person or organization
obtaining a copy of the software and accompanying documentation covered by
this license (the "Software") to use, reproduce, display, distribute,
execute, and transmit the Software, and to prepare derivative works of the
Software, and to permit third-parties to whom the Software is furnished to
do so, all subject to the following:

The copyright notices in the Software and this entire statement, including
the above license grant, this restriction and the following disclaimer,
must be included in all copies of the Software, in whole or in part, and
all derivative works of the Software, unless such copies or derivative
works are solely in the form of machine-executable object code generated by
a source language processor.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

## BSD 2-Clause component

Copyright 2022 Tim Buktu.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

Build plugins, test libraries, benchmarks, and dependencies of library modules
that are not bundled in the CLI archives are not redistributed by those
archives. Their licenses remain available from their published metadata and
upstream projects.
