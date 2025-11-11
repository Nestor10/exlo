# Getting Started with EXLO

Build your first EXLO connector in 3 steps.

## What You'll Build

A simple connector that emits hardcoded data to demonstrate the basic pattern.

## Step 1: Extend ExloApp

```scala
object MyConnector extends ExloApp
```

## Step 2: Implement Three Methods

- `connectorId` - Unique identifier
- `connectorVersion` - Semantic version  
- `extract(state)` - Your extraction logic

## Step 3: Emit StreamElements

Emit `Data` for records and `Checkpoint` for state.

## Complete Example

See: `/examples/src/main/scala/SimpleConnector.scala`

Run: `sbt "examples/runMain examples.SimpleConnector"`

## Next Steps

- [Pagination Pattern](./pagination.md)
- [Adding Dependencies](./dependencies.md)
- [Configuration](./configuration.md)
