import asyncio
from asyncftx import Client


class TestApi:
    def __init__(self, key: str = None, secret: str = None):
        self.key = key
        self.secret = secret
        self.api = Client(key, secret)

    async def test_public(self):
        print('Testing public functions ... ')
        return await self.api.list_markets()

    async def test_private(self):
        print('Testing private functions ... ')
        return await self.api.get_balances()


key = ''
secret = ''
async def main():
    api = TestApi(key,secret)
    ret = await api.test_public()
    print(ret)
    ret = await api.test_private()
    print(ret)

asyncio.run(main())
