import random
from typing import Any
import sys
import faust
from faust.serializers.codecs import codecs
from faust.types import ProcessingGuarantee
from mode import CrashingSupervisor
import os

brokers = "127.0.0.1:9094"
broker_list = list(map(lambda x: "kafka://" + str(x), brokers.split(",")))

settings = faust.Settings(
    id="test-faust-config",
    processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
    agent_supervisor=CrashingSupervisor,
    broker=broker_list,
    broker_commit_interval=0.1,
    topic_disable_leader=True
)

app = faust.App(id="test-faust-config")

settings.web_port = random.randint(6066,7000)

app.conf = settings


topic_input = app.topic(
    "fake-messages",
    key_serializer=codecs["raw"],
    key_type=bytes,
    value_serializer=codecs["raw"],

)

topic_output = app.topic(
    "processed-messages",
    key_serializer=codecs["raw"],
    key_type=bytes,
    value_serializer=codecs["raw"],
)



@app.agent(topic_input)
async def processor(stream: faust.Stream[Any]):
    async for key, value in stream.items():
        #print(key)
        await topic_output.send(key=key, value=value)


sys.argv = ["", "worker", "-l", "info"]
app.main()
