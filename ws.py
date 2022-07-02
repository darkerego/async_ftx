import asyncio
import json

from websockets import connect


class EchoWebsocket:
    async def __aenter__(self):
        self._conn = connect("wss://echo.websocket.org")
        self.websocket = await self._conn.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._conn.__aexit__(*args, **kwargs)

    async def send(self, message):
        await self.websocket.send(message)

    async def send_json(self, message):
        await self.send(json.dumps(message))

    async def receive(self):
        return await self.websocket.recv()


async def main():
    async with EchoWebsocket() as echo:
        await echo.send("Hello!")
        print(await echo.receive())  # "Hello!"


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())