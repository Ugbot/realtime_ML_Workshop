
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import connection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from pydantic import BaseModel

from mastodon.mastodon_types import MastodonPost, MastodonAccount


class PostDataMapper(Model):
    account_id = columns.Text(primary_key=True, partition_key=True)
    id = columns.Text(primary_key=True, clustering_order='DESC')
    created_at = columns.DateTime(primary_key=True, clustering_order='DESC')
    card = columns.Map(columns.Text, columns.Text)
    content = columns.Text()
    edited_at = columns.Text()
    emojis = columns.List(columns.Text)
    favourites_count = columns.Integer()
    in_reply_to_account_id = columns.Text()
    in_reply_to_id = columns.Text()
    language = columns.Text()
    media_attachments = columns.List(columns.Text)
    mentions = columns.List(columns.Text)
    poll = columns.Map(columns.Text, columns.Text)
    reblog = columns.Map(columns.Text, columns.Text)
    reblogs_count = columns.Integer()
    replies_count = columns.Integer()
    sensitive = columns.Boolean()
    spoiler_text = columns.Text()
    tags = columns.List(columns.Text)
    uri = columns.Text()
    url = columns.Text()
    visibility = columns.Text()

    @classmethod
    def save_post(cls, post: MastodonPost):
        post_data = post.dict()
        post_data['created_at'] = datetime.strptime(post_data['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')

        account_id = post_data['account']['id']
        obj = cls(account_id=account_id, **post_data)
        obj.save()

    @classmethod
    def get_post_by_id(cls, account_id: str, post_id: str):
        return cls.objects.filter(account_id=account_id, id=post_id).all()

class AccountDataMapper(Model):
    id = columns.Text(primary_key=True)
    acct = columns.Text(primary_key=True)
    username = columns.Text(primary_key=True)
    avatar = columns.Text()
    avatar_static = columns.Text()
    bot = columns.Boolean()
    created_at = columns.Text()
    discoverable = columns.Boolean()
    display_name = columns.Text()
    emojis = columns.List(columns.Text)
    fields = columns.List(columns.Text)
    followers_count = columns.Integer()
    following_count = columns.Integer()
    group = columns.Boolean()
    header = columns.Text()
    header_static = columns.Text()
    last_status_at = columns.Text()
    locked = columns.Boolean()
    note = columns.Text()
    statuses_count = columns.Integer()
    url = columns.Text()

    @classmethod
    def save_account(cls, account: MastodonAccount):
        account_data = account.dict()
        obj = cls(**account_data)
        obj.save()

    @classmethod
    def get_account_by_id(cls, account_id: str):
        return cls.objects.filter(id=account_id).all()

    @classmethod
    def get_account_by_username(cls, username: str):
        return cls.objects.filter(username=username).all()