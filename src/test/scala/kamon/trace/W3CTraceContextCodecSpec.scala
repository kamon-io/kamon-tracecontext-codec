package kamon.trace

import kamon.Kamon
import kamon.context.{ Context, TextMap }
import kamon.trace.SpanContext.SamplingDecision
import org.scalatest.{ Matchers, OptionValues, WordSpec }

class W3CTraceContextCodecSpec extends WordSpec with Matchers with OptionValues {

  "The W3C TraceContext codecs" when {
    "decoding a Context" should {
      "recognize our parent Span when in traceparent" in {
        val context = decode(
          "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )

        val parentSpan = context.get(Span.ContextKey)
        parentSpan.context().traceID.string shouldBe "0af7651916cd43dd8448eb211c80319c"
        parentSpan.context().spanID.string shouldBe "b7ad6b7169203331"
        parentSpan.context().parentID shouldBe IdentityProvider.NoIdentifier
        parentSpan.context().samplingDecision shouldBe SamplingDecision.Sample
      }

      "recognize our parent Span when in tracestate" in {
        val context = decode(
          "00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01",
          "rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01," +
          "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )

        val parentSpan = context.get(Span.ContextKey)
        parentSpan.context().traceID.string shouldBe "0af7651916cd43dd8448eb211c80319c"
        parentSpan.context().spanID.string shouldBe "b7ad6b7169203331"
        parentSpan.context().parentID shouldBe IdentityProvider.NoIdentifier
        parentSpan.context().samplingDecision shouldBe SamplingDecision.Sample
      }

      "read all entries from tracestate when we own the current traceparent" in {
        val context = decode(
          "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )

        val traceState = context.get(W3CTraceContext.TraceStateKey).value
        traceState.parentOwner shouldBe "kamon"
        traceState.parent.context().traceID.string shouldBe "0af7651916cd43dd8448eb211c80319c"
        traceState.parent.context().spanID.string shouldBe "b7ad6b7169203331"
        traceState.parent.context().parentID shouldBe IdentityProvider.NoIdentifier
        traceState.parent.context().samplingDecision shouldBe SamplingDecision.Sample
        traceState.state should contain allOf(
          "kamon" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "rojo" -> "00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01",
          "blue" -> "lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )
      }

      "read all entries from tracestate when the current traceparent is owned by another system" in {
        val context = decode(
          "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "rojo=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "kamon=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )

        val traceState = context.get(W3CTraceContext.TraceStateKey).value
        traceState.parentOwner shouldBe "rojo"
        traceState.parent.context().traceID.string shouldBe "0af7651916cd43dd8448eb211c80319c"
        traceState.parent.context().spanID.string shouldBe "b7ad6b7169203331"
        traceState.parent.context().parentID shouldBe IdentityProvider.NoIdentifier
        traceState.parent.context().samplingDecision shouldBe SamplingDecision.Sample
        traceState.state should contain allOf(
          "rojo" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "kamon" -> "00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01",
          "blue" -> "lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )

        //      val packedContext = encode(context)
        //      packedContext.values.toSeq should contain allOf (
        //        "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        //        "tracestate" -> "kamon=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01,rojo=00-8448eb211c80319c0af7651916cd43dd-69203331b7ad6b71-01,blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        //      )
      }
    }

    "encoding a Context" should {
      "write the first traceparent and tracestate headers when no previous TraceContext was present" in {
        val packedContext = encode(
          Context.create(Span.ContextKey, newSpan("0af7651916cd43dd8448eb211c80319c", "0011223344556677", SamplingDecision.Sample))
        )

        packedContext.values.toSeq should contain allOf (
          "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01",
          "tracestate" -> "kamon=00-0af7651916cd43dd8448eb211c80319c-0011223344556677-01"
        )
      }

      "forward traceparent and tracestate, even if there is no local tracing" in {
        val packedContext = encode(decode(
          "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "rojo=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01," +
          "blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        ))

        packedContext.values.toSeq should contain allOf (
          "traceparent" -> "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
          "tracestate" -> "rojo=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01,blue=lZWRzIHRoNhcm5hbCBwbGVhc3VyZS4="
        )
      }
    }
  }

  def encode(context: Context): TextMap =
    Kamon.contextCodec().HttpHeaders.encode(context)

  def decode(traceParent: String, traceState: String): Context = {
    decode(Map(
      W3CTraceContext.TraceParentHeaderName -> traceParent,
      W3CTraceContext.TraceStateHeaderName -> traceState
    ))
  }

  def decode(headers: Map[String, String]): Context = {
    val textMap = new TextMap {
      override def values: Iterator[(String, String)] = headers.iterator
      override def put(key: String, value: String): Unit = sys.error("not allowed to write anything")
      override def get(key: String): Option[String] = headers.get(key)
    }

    Kamon.contextCodec().HttpHeaders.decode(textMap)
  }

  def newSpan(traceID: String, spanID: String, samplingDecision: SamplingDecision): Span =
    Span.Remote(SpanContext(
      traceID = Kamon.identityProvider.traceIdGenerator().from(traceID),
      spanID = Kamon.identityProvider.spanIdGenerator().from(spanID),
      parentID = IdentityProvider.NoIdentifier,
      samplingDecision
    ))

}
