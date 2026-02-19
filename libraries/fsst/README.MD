This implementation tries to be a best-of-breed given existing implementation. We rely on 3 implementations:
- C++ (original) - https://github.com/cwida/fsst
- Go - https://github.com/axiomhq/fsst
- Java - https://github.com/maplibre/maplibre-tile-spec

As of 0.0.15:
- The CPU performance looks acceptable (less than 10x slower than C++)
- The RAM performance looks acceptable
- The compression ration looks acceptable (similar to C++)
- The stability looks good (no know issues).
- The API is still immature.

# C++ Original

This is supposedly the most efficient ((de)-compression rate, compression ratio in wider cases) implementation.

Original Article: [VLDB](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf)

It implements some mechanisms we do not implement:
- zeroterminated: not implemented as supposedly irrelevant in Java world
- AVSX12 optimizations
- 12-bytes codes

# Go port

It relies on a sparse list of integer-pairs to collect consecutive codes while training. We should investigate if it relates to one of the mode ic C++.

# Java port

We discovered `maplibre-tile-spec` port quite lately, hence a large effort was already done in our own implementation. Also, this is not yet available as a library, but it is integrated into a rather large project. Finally, it is focused into `training_encode` a single `String`, instead of training given multiple `String` then `encoding` each `String` one at a time.


- https://github.com/maplibre/maplibre-tile-spec/tree/main/java/mlt-core/src/main/java/org/maplibre/mlt/converter/encodings/fsst
  - https://github.com/maplibre/maplibre-tile-spec/issues/150
  - https://github.com/maplibre/maplibre-tile-spec/pull/251

# Roadmap

- Enable configuration of parameters. As the decoding is straightforward, one may be willing to customize some training parameters.
- Enable running literals. FSST has a reserved symbol for one literal encoding. It seems legitimate to enable another symbol for running length decoding. It might be compatible with FSST with a different version.
- Enable longer symbols. FSST enable 12-bytes alternative scheme.