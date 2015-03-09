import javax.servlet.ServletContext
import org.scalatra.LifeCycle
import org.scalatra.servlet._

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) = {
    context.mount(new kamon.example.KamonServlet, "/test")
  }
}
