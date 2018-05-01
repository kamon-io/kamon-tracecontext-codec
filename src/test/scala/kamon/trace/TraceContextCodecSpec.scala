package kamon.trace

import kamon.Kamon
import kamon.context.{ Context, TextMap }
import kamon.trace.SpanContext.SamplingDecision
import org.scalatest.{ Matchers, OptionValues, WordSpec }

class TraceContextCodecSpec extends WordSpec with Matchers with OptionValues {

  "the TraceContext codec decoding functionality" should {
    "pass a tracecontext through Kamon's context without modifications" in {
      val context = decode(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
      )

      val parentSpan = context.get(Span.ContextKey)
      val traceState = context.get(TraceContext.TraceState)

      parentSpan.context().traceID.string shouldBe "0af7651916cd43dd8448eb211c80319c"
      parentSpan.context().spanID.string shouldBe "b7ad6b7169203331"
      parentSpan.context().parentID shouldBe IdentityProvider.NoIdentifier
      parentSpan.context().samplingDecision shouldBe SamplingDecision.Sample

      traceState.value.state should contain allOf (
        "kamon" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "rojo" -> "00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01",
        "blue" -> "lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
      )

      val packedContext = encode(context)
      packedContext.values.toSeq should contain allOf (
        "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "tracestate" -> "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01,rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01,blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
      )
    }

    "take the leading tracestate position when a new traceparent is written" in {
      val context = decode(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01,kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
      )

      val packedContext = encode(
        context.withKey(Span.ContextKey, newSpan("0af7651916cd43dd8448eb211c80319c", "0011223344556677", SamplingDecision.Sample))
      )

      packedContext.values.toSeq should contain allOf (
        "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01",
        "tracestate" -> "kamon=00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01,rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01"
      )
    }

    "create the traceparent and tracestate headers if there was not previous state" in {
      val spanOnlyContext = Context.create(Span.ContextKey, newSpan("0af7651916cd43dd8448eb211c80319c", "0011223344556677", SamplingDecision.Sample))
      val packedContext = encode(spanOnlyContext)

      packedContext.values.toSeq should contain allOf (
        "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01",
        "tracestate" -> "kamon=00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01"
      )
    }

  }

  def encode(context: Context): TextMap =
    (new TraceContext()).encode(context)

  def decode(traceParent: String, traceState: String): Context = {
    decode(Map(
      TraceContext.TraceParentHeader -> traceParent,
      TraceContext.TraceStateHeader -> traceState
    ))
  }

  def decode(headers: Map[String, String]): Context = {
    val codec = new TraceContext()
    val textMap = new TextMap {
      override def values: Iterator[(String, String)] = headers.iterator
      override def put(key: String, value: String): Unit = sys.error("not allowed to write anything")
      override def get(key: String): Option[String] = headers.get(key)
    }

    codec.decode(textMap, Context.Empty)
  }

  def newSpan(traceID: String, spanID: String, samplingDecision: SamplingDecision): Span =
    Span.Remote(SpanContext(
      traceID = Kamon.identityProvider.traceIdGenerator().from(traceID),
      spanID = Kamon.identityProvider.spanIdGenerator().from(spanID),
      parentID = IdentityProvider.NoIdentifier,
      samplingDecision
    ))

}
