# Incremental Sync

Fetch only data that changed since last extraction.

## Pattern Overview

Use timestamps or IDs to track what's been extracted, fetch only new/updated records.

## Key Concepts

1. **Parse checkpoint** to get last timestamp/ID
2. **Fetch records since** that point
3. **Emit all records**
4. **Save latest timestamp/ID** as checkpoint

## Complete Example

See: `/examples/src/main/scala/IncrementalConnector.scala`

Run: `sbt "examples/runMain examples.IncrementalConnector"`

## State Formats

### Timestamp-based
```json
{"lastTimestamp": "2024-01-15T10:30:00Z"}
```

### ID-based
```json
{"lastProcessedId": 12345}
```

### Combined
```json
{
  "lastTimestamp": "2024-01-15T10:30:00Z",
  "lastProcessedId": 12345
}
```

## Best Practice

Always include both timestamp and ID in state when possible. This provides redundancy if API behavior changes.

## Related

- [State Management](./state-management.md)
- [Pagination](./pagination.md)
