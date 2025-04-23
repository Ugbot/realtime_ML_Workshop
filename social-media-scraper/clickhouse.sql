CREATE TABLE post_table (
    id String,
    uri String,
    url String,
    account_id String,
    username String,
    in_reply_to_id String,
    in_reply_to_account_id String,
    reblog Map(String, String),
    content String,
    created_at Int64,
    emojis Array(String),
    replies_count Int32,
    reblogs_count Int32,
    favourites_count Int32,
    reblogged UInt8,
    favourited UInt8,
    muted UInt8,
    sensitive UInt8,
    spoiler_text String,
    visibility String,
    media_attachments Array(Map(String, String)),
    mentions Array(Map(String, String)),
    tags Array(Map(String, String)),
    card Map(String, String),
    poll Map(String, String),
    application Map(String, String),
    language String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'posts',
    kafka_group_name = 'post_consumer',
    kafka_format = 'JSONEachRow';

--##################################
CREATE TABLE account_table (
    id String,
    username String,
    acct String,
    display_name String,
    locked UInt8,
    created_at DateTime64(3),
    followers_count Int32,
    following_count Int32,
    statuses_count Int32,
    note String,
    url String,
    avatar String,
    avatar_static String,
    header String,
    header_static String,
    emojis Array(Map(String, String)),
    fields Array(Map(String, String)),
    bot UInt8,
    source Map(String, String),
    profile Map(String, String),
    last_status_at String,
    discoverable UInt8,
    `group` UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'accounts_json',
    kafka_group_name = 'account_consumer',
    kafka_format = 'JSONEachRow';

--##########################


CREATE MATERIALIZED VIEW mv_just_posts_table
ENGINE = MergeTree()
ORDER BY (created_at)
AS
SELECT *
FROM post_table;
