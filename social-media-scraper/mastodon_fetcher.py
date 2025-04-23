import httpx
import asyncio

from mastodon_types import MastodonPost


class MastodonFetcher:
    def __init__(self, server, interval_seconds, callback):
        self.server = server
        self.interval_seconds = interval_seconds
        self.callback = callback
        self.min_id = None

    async def fetch_timeline(self):
        url = f"{self.server}/api/v1/timelines/public"
        params = {"min_id": self.min_id} if self.min_id else None

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    async def run(self):
        try:
            while True:
                timeline = await self.fetch_timeline()
                for status in timeline:
                    my_toot = MastodonPost(**status)
                    self.callback(my_toot)
                    self.min_id = my_toot.id
                await asyncio.sleep(self.interval_seconds)
        except httpx.HTTPError as exc:
            print(f"An error occurred: {exc}")


# # Example usage:
# def handle_message(content):
#     print(f"Received message: {content}")
#
#
# fetcher = MastodonFetcher("https://mastodon.social", 10, handle_message)
# asyncio.run(fetcher.run())
