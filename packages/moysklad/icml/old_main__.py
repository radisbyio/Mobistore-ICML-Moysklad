import asyncio


async def test():
    print('Hello ...')
    await asyncio.sleep(1)
    print('... World!')


def main(args):
    asyncio.run(test())
    name = args.get("name", "stranger")
    greeting = "Hello " + name + "!"
    print(greeting)
    return {"body": greeting}
