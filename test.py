import asyncio
import pynats
import signal
import logging

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()


con = pynats.Connection()
connection_setup = asyncio.async(con.connect(), loop=loop)

@asyncio.coroutine
def stop_loop():
    loop.stop()

def ask_exit():
    asyncio.async(con.close())
    asyncio.async(stop_loop())

for signame in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(getattr(signal, signame), ask_exit)

@asyncio.coroutine
def ping():
    yield from connection_setup
    while True:
        yield from asyncio.sleep(1.0, loop=loop)
        yield from con.ping()
        con.publish('topic', '今日は')

asyncio.async(ping(), loop=loop)

def subscription_callback(msg):
    pass

@asyncio.coroutine
def subscribe():
    yield from connection_setup
    con.subscribe('topic', subscription_callback)

asyncio.async(subscribe(), loop=loop)

try:
    loop.run_forever()
finally:
    loop.close()

