CREATE TABLE post_table (
    id STRING,
    uri STRING,
    url STRING,
    account ROW<id STRING,
        username STRING,
        acct STRING,
        display_name STRING,
        locked BOOLEAN,
        created_at TIMESTAMP(3),
        followers_count INT,
        following_count INT,
        statuses_count INT,
        note STRING,
        url STRING,
        avatar STRING,
        avatar_static STRING,
        header STRING,
        header_static STRING,
        emojis ARRAY<MAP<STRING, STRING>>,
        fields ARRAY<MAP<STRING, STRING>>,
        bot BOOLEAN,
        source MAP<STRING, STRING>,
        profile MAP<STRING, STRING>,
        last_status_at STRING,
        discoverable BOOLEAN,
        `group` BOOLEAN>,
    in_reply_to_id STRING,
    in_reply_to_account_id STRING,
    reblog MAP<STRING, STRING>,
    content STRING,
    created_at TIMESTAMP(3),
    emojis ARRAY<STRING>,
    replies_count INT,
    reblogs_count INT,
    favourites_count INT,
    reblogged BOOLEAN,
    favourited BOOLEAN,
    muted BOOLEAN,
    `sensitive` BOOLEAN,
    spoiler_text STRING,
    visibility STRING,
    media_attachments ARRAY<MAP<STRING, STRING>>,
    mentions ARRAY<MAP<STRING, STRING>>,
    tags ARRAY<MAP<STRING, STRING>>,
    card MAP<STRING, STRING>,
    poll MAP<STRING, STRING>,
    application MAP<STRING, STRING>,
    `language` STRING
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '',
    'scan.startup.mode' = 'earliest-offset',
    'topic' = 'complete-json',
    'value.format' = 'json'
)


CREATE TABLE account_table (
    id STRING,
    username STRING,
    acct STRING,
    display_name STRING,
    locked BOOLEAN,
    created_at TIMESTAMP(3),
    followers_count INT,
    following_count INT,
    statuses_count INT,
    note STRING,
    url STRING,
    avatar STRING,
    avatar_static STRING,
    header STRING,
    header_static STRING,
    emojis ARRAY<MAP<STRING, STRING>>,
    fields ARRAY<MAP<STRING, STRING>>,
    bot BOOLEAN,
    source MAP<STRING, STRING>,
    profile MAP<STRING, STRING>,
    last_status_at STRING,
    discoverable BOOLEAN,
    `group` BOOLEAN
)


--Attempt 1
SELECT
    TUMBLE_END(created_at, INTERVAL '30' SECOND) AS window_end,
    id,
    content,
    favourites_count
FROM
    PostTable
GROUP BY
    TUMBLE(created_at, INTERVAL '30' SECOND),
    id,
    content,
    favourites_count
ORDER BY
    favourites_count DESC;


--Attempt 2
SELECT
    TUMBLE_END(created_at, INTERVAL '30' SECOND) AS window_end,
    id,
    content,
    favourites_count
FROM
    PostTable
GROUP BY
    TUMBLE(created_at, INTERVAL '30' SECOND),
    id,
    content,
    favourites_count
ORDER BY
    favourites_count DESC
LIMIT 10;

--Attempt 3
SELECT
    window_end,
    id,
    content,
    favourites_count
FROM (
    SELECT
        TUMBLE_END(created_at, INTERVAL '30' SECOND) AS window_end,
        id,
        content,
        favourites_count,
        ROW_NUMBER() OVER (PARTITION BY TUMBLE_END(created_at, INTERVAL '30' SECOND) ORDER BY favourites_count DESC) as row_num
    FROM
        PostTable
)
WHERE row_num <= 10;



--######################### current input table

CREATE TABLE post_table (
    id STRING,
    uri STRING,
    url STRING,
    account ROW<id STRING,
        username STRING,
        acct STRING,
        display_name STRING,
        locked BOOLEAN,
        created_at BIGINT,
        followers_count INT,
        following_count INT,
        statuses_count INT,
        note STRING,
        url STRING,
        avatar STRING,
        avatar_static STRING,
        header STRING,
        header_static STRING,
        emojis ARRAY<MAP<STRING, STRING>>,
        fields ARRAY<MAP<STRING, STRING>>,
        bot BOOLEAN,
        source MAP<STRING, STRING>,
        profile MAP<STRING, STRING>,
        last_status_at STRING,
        discoverable BOOLEAN,
        `group` BOOLEAN>,
    in_reply_to_id STRING,
    in_reply_to_account_id STRING,
    reblog MAP<STRING, STRING>,
    content STRING,
    created_at BIGINT,
    emojis ARRAY<STRING>,
    replies_count INT,
    reblogs_count INT,
    favourites_count INT,
    reblogged BOOLEAN,
    favourited BOOLEAN,
    muted BOOLEAN,
    `sensitive` BOOLEAN,
    spoiler_text STRING,
    visibility STRING,
    media_attachments ARRAY<MAP<STRING, STRING>>,
    mentions ARRAY<MAP<STRING, STRING>>,
    tags ARRAY<MAP<STRING, STRING>>,
    card MAP<STRING, STRING>,
    poll MAP<STRING, STRING>,
    application MAP<STRING, STRING>,
    `language` STRING
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '',
    'scan.startup.mode' = 'earliest-offset',
    'topic' = 'easy-timestamps',
    'value.format' = 'json'
)

--######################## current sink table accounts

CREATE TABLE account_table (
    id STRING,
    username STRING,
    acct STRING,
    display_name STRING,
    locked BOOLEAN,
    created_at TIMESTAMP(3),
    followers_count INT,
    following_count INT,
    statuses_count INT,
    note STRING,
    url STRING,
    avatar STRING,
    avatar_static STRING,
    header STRING,
    header_static STRING,
    emojis ARRAY<MAP<STRING, STRING>>,
    fields ARRAY<MAP<STRING, STRING>>,
    bot BOOLEAN,
    source MAP<STRING, STRING>,
    profile MAP<STRING, STRING>,
    last_status_at STRING,
    discoverable BOOLEAN,
    `group` BOOLEAN
)WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '',
    'scan.startup.mode' = 'earliest-offset',
    'topic' = 'accounts_json',
    'value.format' = 'json'
)

--######## select int oaccounts

INSERT INTO account_table
SELECT
    account.id,
    account.username,
    account.acct,
    account.display_name,
    account.locked,
    TO_TIMESTAMP_LTZ(account.created_at,3),
    account.followers_count,
    account.following_count,
    account.statuses_count,
    account.note,
    account.url,
    account.avatar,
    account.avatar_static,
    account.header,
    account.header_static,
    account.emojis,
    account.fields,
    account.bot,
    account.source,
    account.profile,
    account.last_status_at,
    account.discoverable,
    account.`group`
FROM
    post_table;

--######## just the posts

CREATE TABLE just_posts_table (
    id STRING,
    uri STRING,
    url STRING,
    account_id STRING,
    username STRING,
    in_reply_to_id STRING,
    in_reply_to_account_id STRING,
    reblog MAP<STRING, STRING>,
    content STRING,
    created_at TIMESTAMP(3),
    emojis ARRAY<STRING>,
    replies_count INT,
    reblogs_count INT,
    favourites_count INT,
    reblogged BOOLEAN,
    favourited BOOLEAN,
    muted BOOLEAN,
    `sensitive` BOOLEAN,
    spoiler_text STRING,
    visibility STRING,
    media_attachments ARRAY<MAP<STRING, STRING>>,
    mentions ARRAY<MAP<STRING, STRING>>,
    tags ARRAY<MAP<STRING, STRING>>,
    card MAP<STRING, STRING>,
    poll MAP<STRING, STRING>,
    application MAP<STRING, STRING>,
    `language` STRING
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '',
    'scan.startup.mode' = 'earliest-offset',
    'topic' = 'posts',
    'value.format' = 'json'
)

--######################## creating sql for just_posts

INSERT INTO just_posts_table
SELECT
    id,
    uri,
    url,
    account.id AS account_id,
    account.username,
    in_reply_to_id,
    in_reply_to_account_id,
    reblog,
    content,
    TO_TIMESTAMP_LTZ(created_at,3),
    emojis,
    replies_count,
    reblogs_count,
    favourites_count,
    reblogged,
    favourited,
    muted,
    `sensitive`,
    spoiler_text,
    visibility,
    media_attachments,
    mentions,
    tags,
    card,
    poll,
    application,
    `language`
FROM
    post_table;


--##############################create Opeserach index

INSERT INTO elasticsearch7
SELECT
    id,
    uri,
    url,
    account_id,
    username,
    in_reply_to_id,
    in_reply_to_account_id,
    reblog,
    content,
    DATE_FORMAT(created_at, 'yyyy/MM/dd HH:mm:ss'),
    emojis,
    replies_count,
    reblogs_count,
    favourites_count,
    reblogged,
    favourited,
    muted,
    `sensitive`,
    spoiler_text,
    visibility,
    media_attachments,
    mentions,
    tags,
    card,
    poll,
    application,
    `language`
FROM
    just_posts_table