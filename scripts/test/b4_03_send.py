import json
import sys
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

SOURCE = "ecommerce_events_b4_oct22_v1"
TARGET = "ecommerce_events_b4_restart_v1"
SESSION = "001226df-03c7-41e6-a9c4-7d0233348bd5"
BROKER = "localhost:9092"

phase = sys.argv[1] if len(sys.argv) == 2 else ""
if phase not in ("phase1", "phase2"):
    sys.exit("Usage: python3 b4_03_send.py phase1|phase2")

consumer = KafkaConsumer(
    bootstrap_servers=BROKER,
    group_id=None,
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
)

parts = consumer.partitions_for_topic(SOURCE)
if not parts:
    sys.exit("ERROR: Source topic unavailable")

tps = [TopicPartition(SOURCE, p) for p in sorted(parts)]
consumer.assign(tps)
consumer.seek_to_beginning(*tps)

events = []
for msg in consumer:
    try:
        data = json.loads(msg.value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        continue
    if data.get("user_session") == SESSION:
        events.append((msg, data))

consumer.close()

def order(item):
    data = item[1]
    return (
        str(data["event_time"]),
        int(data["event_type_rank"]),
        str(data["event_sort_key"]),
    )

events.sort(key=order)

def first_after(kind, previous=None):
    for item in events:
        if item[1].get("event_type") != kind:
            continue
        if previous is None or order(item) > order(previous):
            return item
    raise RuntimeError("Missing valid stage: " + kind)

view = first_after("view")
cart = first_after("cart", view)
purchase = first_after("purchase", cart)

selected = (
    [view, purchase] if phase == "phase1" else [cart]
)

# Chan gui lap: phase1 can topic rong;
# phase2 can dung 2 offsets da gui.
check = KafkaConsumer(
    bootstrap_servers=BROKER,
    group_id=None,
)
tp = TopicPartition(TARGET, 0)
begin = check.beginning_offsets([tp])[tp]
end = check.end_offsets([tp])[tp]
check.close()

expected_end = 0 if phase == "phase1" else 2
if begin != 0 or end != expected_end:
    sys.exit(
        "STOP: target offsets unexpected: "
        "begin={}, end={}, expected_end={}".format(
            begin, end, expected_end
        )
    )

producer = KafkaProducer(bootstrap_servers=BROKER)

for msg, data in selected:
    ack = producer.send(
        TARGET,
        key=msg.key,
        value=msg.value,
    ).get(timeout=30)

    print(
        "ACK phase={} type={} time={} "
        "partition={} offset={}".format(
            phase,
            data["event_type"],
            data["event_time"],
            ack.partition,
            ack.offset,
        )
    )

producer.flush()
producer.close()
print("SEND_CHECK=PASS")
