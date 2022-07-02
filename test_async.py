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



async def main():
    api = TestApi('9cFo0M3OdYf_ksVUybVae-u6ithk_qFniHPwmX2l', "sbPfaO9VUI4z79nH5zcoDJJqeIRwpz-Mx-2dQPHu")
    ret = await api.test_public()
    print(ret)
    ret = await api.test_private()
    print(ret)

asyncio.run(main())
