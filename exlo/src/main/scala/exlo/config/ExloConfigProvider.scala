package exlo.config

import zio.ConfigProvider
import zio.config.typesafe.TypesafeConfigProvider

/** Custom ConfigProvider for EXLO with proper SCREAMING_SNAKE_CASE handling.
  *
  * Uses snakeCase conversion then uppercases, with a simple fix for known
  * acronyms that get split.
  */
object ExloConfigProvider:

  /** Default ConfigProvider for EXLO applications.
    *
    * Tries environment variables first (with SCREAMING_SNAKE_CASE), then falls
    * back to application.conf from classpath (with kebab-case).
    *
    * This is the recommended provider for production use.
    */
  def default: ConfigProvider =
    envProvider.orElse(
      TypesafeConfigProvider.fromResourcePath().kebabCase
    )

  /** Environment variable provider with SCREAMING_SNAKE_CASE naming.
    *
    * Uses underscore as the path delimiter and converts all path segments to
    * SCREAMING_SNAKE_CASE. Most conversions are handled by snakeCase.upperCase,
    * with a simple post-processing step to fix acronyms that get incorrectly
    * split (e.g., S3 → S_3 needs to become S3).
    *
    * Example mappings:
    *   - exlo.storage.warehousePath → EXLO_STORAGE_WAREHOUSE_PATH
    *   - exlo.storage.backend.S3.region → EXLO_STORAGE_BACKEND_S3_REGION
    *   - exlo.storage.backend.Gcs.projectId →
    *     EXLO_STORAGE_BACKEND_GCS_PROJECT_ID
    */
  def envProvider: ConfigProvider =
    ConfigProvider.envProvider
      .snakeCase
      .upperCase
      .contramapPath(fixAcronyms)

  /** Create a ConfigProvider from a map with SCREAMING_SNAKE_CASE naming.
    *
    * Useful for testing. The map keys should use the same SCREAMING_SNAKE_CASE
    * format as environment variables.
    */
  def fromMap(
      map: Map[String, String],
      pathDelim: String = "_",
      seqDelim: String = ","
  ): ConfigProvider =
    ConfigProvider
      .fromMap(map, pathDelim, seqDelim)
      .snakeCase
      .upperCase
      .contramapPath(fixAcronyms)

  /** Fix known acronyms that get incorrectly split by snakeCase.
    *
    * snakeCase converts:
    *   - S3 → s_3 (then upperCase → S_3, we want S3) 
    *   - Gcs → gcs (then upperCase → GCS, already correct)
    *
    * This simply fixes the known problematic cases.
    */
  private def fixAcronyms(name: String): String =
    name
      .replace("S_3", "S3")
      // Add more if needed, but most acronyms work fine (GCS, JDBC, etc.)


