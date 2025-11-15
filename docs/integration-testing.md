# Running Integration Tests with External APIs

Some integration tests require API credentials to test against real services.

## Setup

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Fill in your API keys in `.env`:
   ```bash
   # Get a free key at https://newsapi.org/register
   NEWSAPI_API_KEY=your-actual-key-here
   EXLO_CONNECTOR_CONFIG={"api_key":"your-actual-key-here","country":"us","category":"general"}
   ```

3. Run the integration tests (sbt-dotenv automatically loads `.env`):
   ```bash
   sbt "exloYaml/testOnly exlo.yaml.integration.EndToEndIntegrationSpec"
   ```

## How It Works

The project uses [sbt-dotenv](https://github.com/mefellows/sbt-dotenv) to automatically load environment variables from `.env` files:

- `.env` is loaded automatically when you run sbt
- No need to manually `source` the file
- Environment variables are available to all tests

## Test Behavior

- **With API key**: Tests will execute against real NewsAPI endpoints
- **Without API key**: Tests will skip gracefully with a warning message

This ensures tests can run in CI/CD without credentials while allowing local development with real APIs.

## Security

- Never commit `.env` files (already in `.gitignore`)
- Use `.env.example` to document required variables
- Use free tier API keys for development
- Rotate keys if accidentally exposed
