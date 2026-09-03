# JSON package

Bedrock owns the `JSON::Value`, parser, writer, SAX handler, and utility implementation in this directory. Bedrock also owns the vendored RapidJSON headers in `externalLib/rapidjson`.

Applications that use these APIs consume Bedrock's copy. They do not maintain another implementation.

## Build boundary

Bedrock packages the implementation as `libjson.a`. This archive supports staged deployments for applications that already define the same JSON symbols.

Bedrock links the archive with `--exclude-libs,libjson.a`. This option keeps the strong implementation symbols out of the dynamic symbol table.

Thus, an existing application can load the new Bedrock without a collision with its private JSON implementation. After that deployment, the application can link `libjson.a` and remove its private copy.

This archive is a deployment boundary, not a separate JSON implementation. The complete source package stays in this directory.

`Metrics.cpp` remains in `libstuff.a` for static consumers. Bedrock also links its object directly into the executable. Thus, dynamically loaded applications can use the two metrics entry points without exposing the implementation archive.

The default build runs `checkjsonsymbols`. This target makes sure that Bedrock has the correct JSON exports. To inspect them directly, use this command:

```sh
nm -D --defined-only bedrock | c++filt | grep -E ' [TDBR] JSON::'
```

The only output must be `JSON::setMetricsObserver(...)` and `JSON::reportMetrics(...)`. Inline and template definitions can appear as weak symbols. The JSON implementation must have no strong exported symbols.

## Embedding hook

`setMetricsObserver()` installs one optional process-wide callback. The callback reports parse and serialization time and document size.

An application can use the callback to preserve its JSON counters. The JSON package does not depend on that application.

This callback is a narrow metrics hook. It is not a general observer framework.

## Header dependency

RapidJSON is an implementation dependency and a compile-time API dependency. The public writer and SAX headers expose RapidJSON types.

Thus, consumers must add `externalLib/rapidjson/include` to their include path. This requirement applies when they link Bedrock's JSON archive.
