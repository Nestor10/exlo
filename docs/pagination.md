# Pagination Pattern

How to handle paginated APIs with EXLO.

## Pattern Overview

Use `ZStream.unfoldZIO` to fetch pages iteratively, emitting a checkpoint after each page.

## Key Concepts

1. **Parse state** to get starting page/cursor
2. **Fetch page** from API
3. **Emit records** as `Data` elements
4. **Emit checkpoint** with next page info
5. **Repeat** until no more pages

## Complete Example

See: `/examples/src/main/scala/PaginatedConnector.scala`

Run: `sbt "examples/runMain examples.PaginatedConnector"`

## State Format

```json
{"page": 2}
```

or

```json
{"cursor": "abc123xyz"}
```

## Best Practice

Always checkpoint after each page, not at the end. This ensures resumability if extraction fails mid-stream.

## Related

- [Incremental Sync](./incremental-sync.md)
- [State Management](./state-management.md)
