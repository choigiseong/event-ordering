import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Suppressed
import org.apache.kafka.streams.kstream.TimeWindows
import java.time.Duration
import java.util.Properties

fun main(args: Array<String>) {
    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks-app")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094")
        // Exactly-Once V2 권장
        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
    }

    val builder = StreamsBuilder()

    // clicks 토픽: key=userid, value=“/path?utm=…”
    val clicks = builder.stream<String, String>("clicks")

    // 1) 단순 카운트 (KTable)
    val counts = clicks
        .groupByKey()
        .count(Materialized.`as`("clicks-per-user-store"))

    // 2) 1분 tumbling window 집계 (final 결과만 방출)
//    val windowed = clicks
//        .groupByKey()
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//        .count(Materialized.`as`("clicks-per-user-1m"))
//        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
//        .toStream()
//        .map { windowedKey, count ->
//            // key: userId, value: "start-end=count"
//            val user = windowedKey.key()
//            val window = "${windowedKey.window().startTime()}~${windowedKey.window().endTime()}"
//            user to "$window=$count"
//        }

    // 결과를 clicks_per_user 토픽으로
//    counts.toStream().mapValues { v -> v.toString() }.to("clicks_per_user")
//    windowed.to("clicks_per_user")

    val topology = builder.build()
    val streams = KafkaStreams(topology, props)
    streams.start()

    Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
}