This implementation tries to be a best-of-breed given existing implementation. We rely on 3 implementations:
- C++ (original) - https://github.com/cwida/fsst
- Go - https://github.com/axiomhq/fsst
- Java - 

# C++ - Original

This is supposedly the most efficient ((de)-compression rate, compression ratio in wider cases) implementation.

It implements some mechanisms we do not implement:
- zeroterminated: not implemented as supposedly irrelevant in Java world
- AVSX12 optimizations
- 12-bytes codes

# Go port

It relies on a sparse list of integer-pairs to collect consecutive codes while training. We should investigate if it relates to one of the mode ic C++.

# Java port

We discovered this port quite lately, hence a large effort was already done in our own implementation. Also, this is not yet available as a library, but it is integrated into a rather large project. Finally, it is focused into `training_encode` a single `String`, instead of training given multiple `String` then `encoding` each `String` one at a time.


- https://github.com/maplibre/maplibre-tile-spec/tree/main/java/mlt-core/src/main/java/org/maplibre/mlt/converter/encodings/fsst
  - https://github.com/maplibre/maplibre-tile-spec/issues/150
  - https://github.com/maplibre/maplibre-tile-spec/pull/251
