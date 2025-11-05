package examples

// Shared domain types for examples
// In real usage, these would come from the exlo package
enum StreamElement:
  case Data(json: String)
  case Checkpoint(state: String)
