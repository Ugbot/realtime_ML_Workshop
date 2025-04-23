import httpx
import asyncio

async def fetch_timeline(url, min_id=None):
    params = {"min_id": min_id} if min_id else None

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

async def main():
    instance_url = "https://mastodon.social"  # Replace with your Mastodon instance URL
    timeline_endpoint = f"{instance_url}/api/v1/timelines/public"
    interval_seconds = 10  # Adjust the interval as needed
    min_id = None

    try:
        while True:
            print('fetching.....\n')
            timeline = await fetch_timeline(timeline_endpoint, min_id=min_id)
            for status in timeline:
                print(status['content'])
                min_id = status['id']
            await asyncio.sleep(interval_seconds)
    except httpx.HTTPError as exc:
        print(f"An error occurred: {exc}")

asyncio.run(main())
