package kamon.example

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

object EmbeddedServer {
  def main(args: Array[String]) {
    val server = new Server(8080)
    val context: WebAppContext = new WebAppContext();
    context.setServer(server)
    context.setContextPath("/");
    context.setWar("src/webapp")
    server.setHandler(context);

    try {
      server.start()
      server.join()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        System.exit(1)
     }
   }
 }
}
