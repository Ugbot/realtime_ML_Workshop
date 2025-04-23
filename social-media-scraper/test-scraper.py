from datetime import datetime, timedelta
import json
from pprint import pprint

import pytz
import requests

from mastodon.mastodon_types import MastodonPost

URL = 'https://mastodon.social/api/v1/timelines/public'
params = {
    'limit': 40
}

current_time = datetime.now(pytz.utc)
since = current_time - timedelta(hours=1)
is_end = False
date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
results = []

while True:
    r = requests.get(URL, params=params)
    toots = json.loads(r.text)

    if len(toots) == 0:
        break
    
    for t in toots:
        pprint(t)
        my_toot = MastodonPost(**t)
        # timestamp = datetime.timestamp(t['created_at'], tz='utc')
        if my_toot.created_at <= since:
            is_end = True
            break

        results.append(my_toot)

    if is_end:
        break

    max_id = toots[-1]['id']
    params['max_id'] = max_id

pprint(results[0])
