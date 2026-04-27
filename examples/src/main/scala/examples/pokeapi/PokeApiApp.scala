package examples.pokeapi

import exlo.Exlo
import exlo.http.HttpExtract
import exlo.runtime.{DestinationFactory, SinkConfig, StreamRegistry}
import exlo.runtime.iceberg.IcebergCodecs.given
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/**
 * Pokemon Kalos pokedex — a stateless full-pull connector. The destination is
 * env-configurable: by default logs to stdout (`EXLO_DESTINATION=logging`); set
 * `EXLO_DESTINATION=iceberg` plus the catalog/table env vars to push to a real Iceberg table.
 *
 * Single-stream source — ships exactly one entry (`kalos`) in its `StreamRegistry`. Every
 * deploy must set `EXLO_STREAM=kalos`; there is no special "single-stream" carve-out in
 * the framework.
 *
 * Local Hadoop catalog example:
 * {{{
 *   EXLO_DESTINATION=iceberg \
 *   EXLO_CATALOG_TYPE=hadoop \
 *   EXLO_CATALOG_WAREHOUSE=/tmp/exlo-warehouse \
 *   EXLO_TABLE_NAMESPACE=exlo \
 *   EXLO_TABLE_NAME=pokeapi_kalos \
 *   EXLO_STREAM=kalos \
 *   sbt 'examples/runMain examples.pokeapi.PokeApiApp'
 * }}}
 *
 * AWS Glue example:
 * {{{
 *   EXLO_DESTINATION=iceberg \
 *   EXLO_CATALOG_TYPE=glue \
 *   EXLO_CATALOG_WAREHOUSE=s3://my-lake/warehouse \
 *   EXLO_TABLE_NAMESPACE=exlo \
 *   EXLO_TABLE_NAME=pokeapi_kalos \
 *   EXLO_STREAM=kalos \
 *   AWS_REGION=us-east-1 \
 *   AWS_PROFILE=my-profile \
 *   sbt 'examples/runMain examples.pokeapi.PokeApiApp'
 * }}}
 */
object PokeApiApp extends ZIOAppDefault:

  private val endpoint =
    URL.decode("https://www.pokemon.com/us/api/pokedex/kalos/").toOption.get

  private val parseArray: Response => ZIO[Any, Throwable, List[Json]] = resp =>
    resp.body.asString.flatMap(s =>
      ZIO.fromEither(s.fromJson[Json])
        .mapError(e => new RuntimeException(s"json parse: $e"))
        .flatMap {
          case Json.Arr(items) => ZIO.succeed(items.toList)
          case other =>
            ZIO.fail(new RuntimeException(s"expected array, got ${other.getClass.getSimpleName}"))
        }
    )

  private val kalosConnector = HttpExtract.fullPull
    .request(Request.get(endpoint))
    .parse(parseArray)
    .records(items => Chunk.fromIterable(items.map(_.toJson)))
    .toConnector("pokeapi", "0.1.0")

  /**
   * Single-entry registry. Future Pokemon endpoints (johto, sinnoh, …) plug in here as
   * additional entries — same image, switch via `EXLO_STREAM`.
   */
  val registry: StreamRegistry = new StreamRegistry:
    val streams = Map[String, ZIO[Any, Throwable, Unit]](
      "kalos" -> {
        for
          sinkCfg <- SinkConfig.fromEnv
          _ <- Exlo
                 .run(kalosConnector, (), sinkCfg)
                 .provide(
                   Client.default,
                   DestinationFactory.layer[Unit]("pokeapi")
                 )
        yield ()
      }
    )

  def run = StreamRegistry.runSelected(registry)
