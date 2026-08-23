package org.apache.spark.recovery.e2e

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/**
 * Immutable first-writer-wins binding for identities that must survive a driver restart: the
 * source anchor and the connector write ID. Same CAS primitive as the task commit store.
 */
object DurableBinding {

  def resolve(dir: String, namespace: String, id: String, proposed: String): String = {
    val root = Paths.get(dir)
    Files.createDirectories(root)
    val key = s"$namespace-v1-${id.length}-$id".replaceAll("[^A-Za-z0-9_.-]", "_")
    val target = root.resolve(s"$key.binding")
    if (Files.exists(target)) {
      return new String(Files.readAllBytes(target), StandardCharsets.UTF_8)
    }
    val tmp = root.resolve(s".binding-tmp-${java.util.UUID.randomUUID()}")
    Files.write(tmp, proposed.getBytes(StandardCharsets.UTF_8))
    try {
      Files.createLink(target, tmp)
      Files.deleteIfExists(tmp)
      proposed
    } catch {
      case _: java.nio.file.FileAlreadyExistsException =>
        Files.deleteIfExists(tmp)
        new String(Files.readAllBytes(target), StandardCharsets.UTF_8)
    }
  }
}
