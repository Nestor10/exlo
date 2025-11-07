# EXLO YAML Runtime

Declarative connector framework for EXLO. Define API connectors using YAML specifications instead of writing code.

## Architecture

The YAML runtime interprets declarative YAML specs into functioning EXLO connectors:

```
YAML Spec → YamlSpecLoader → ConnectorSpec ADT → YamlInterpreter → ZStream[StreamElement]
```

### Components

**Domain (ADTs)**:
- `StreamSpec`: Complete stream configuration
- `PaginationStrategy`: Page/offset/cursor pagination
- `Auth`: NoAuth, ApiKey, Bearer token
- `Requester`: HTTP request configuration
- `RecordSelector`: JSONPath extraction + filters

**Services**:
- `YamlSpecLoader`: Load and parse YAML files
- `HttpClient`: Execute HTTP requests (zio-http)
- `TemplateEngine`: Jinja2 rendering (jinjava)
- `ResponseParser`: JSON extraction (circe-optics)
- `Authenticator`: Apply auth headers

**Interpreter**:
- `YamlInterpreter`: Convert ADTs → ZIO effects
- Handles all pagination strategies
- Applies filters and transformations

**Connector**:
- `YamlConnector`: ExloApp implementation
- Wires all services via ZLayer
- Emits StreamElements with checkpoints

## Example YAML Spec

```yaml
streams:
  - name: users
    
    requester:
      url: "https://api.example.com/users"
      method: GET
      headers:
        X-API-Key: "{{ config.api_key }}"
      params:
        limit: "{{ limit }}"
        offset: "{{ offset }}"
      auth:
        type: Bearer
        token: "{{ config.token }}"
    
    recordSelector:
      extractor:
        fieldPath: [data, users]  # Extract from json.data.users
      filter: "{{ record.active }}"  # Only active users
    
    paginator:
      type: OffsetIncrement
      pageSize: 100
```

## Running

```bash
# Compile
sbt "exloYaml/compile"

# Run connector (once EXLO config is set up)
sbt "exloYaml/run"
```
