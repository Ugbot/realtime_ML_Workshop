from typing import Optional, List, Dict

from pydantic import BaseModel, Field, validator
from datetime import datetime, time


# class EpochTimestampField(Field):
#     # Custom Pydantic field to parse incoming timestamps into epoch format
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#
#     def parse_timestamp_to_epoch(self, value):
#         try:
#             # Parse the timestamp to epoch format
#             epoch = int(time.mktime(time.strptime(value, self.format)))
#             return epoch
#         except ValueError:
#             # Raise an error if the timestamp format is invalid
#             raise ValueError(f"Invalid timestamp format for field '{self.name}'")
#

class MastodonAccount(BaseModel):
    id: str
    username: str
    acct: str
    display_name: str
    locked: bool
    created_at: datetime = Field(default_factory=datetime.utcnow)
    followers_count: int
    following_count: int
    statuses_count: int
    note: Optional[str]
    url: str
    avatar: str
    avatar_static: str
    header: str
    header_static: str
    emojis: List[Dict[str, str]]
    fields: List[Dict[str, Optional[str]]]
    bot: bool
    source: Optional[dict]
    profile: Optional[dict]
    last_status_at: Optional[str]
    discoverable: Optional[bool]
    group: Optional[bool]

    class Config:
        json_encoders = {
            datetime: lambda dt: int(dt.timestamp())
        }


class MastodonPost(BaseModel):
    id: str
    uri: str
    url: str
    account: MastodonAccount
    in_reply_to_id: Optional[str]
    in_reply_to_account_id: Optional[str]
    reblog: Optional[dict]
    content: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    emojis: Optional[List[Dict[str, str]]]
    replies_count: int
    reblogs_count: int
    favourites_count: int
    reblogged: Optional[bool]
    favourited: Optional[bool]
    muted: Optional[bool]
    sensitive: bool
    spoiler_text: Optional[str]
    visibility: str
    media_attachments: Optional[List[dict]]
    mentions: Optional[List[dict]]
    tags: Optional[List[dict]]
    card: Optional[dict]
    poll: Optional[dict]
    application: Optional[dict]
    language: Optional[str]

    class Config:
        json_encoders = {
            datetime: lambda dt: int(dt.timestamp())
        }
