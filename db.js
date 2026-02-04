const { Pool } = require('pg');

let connString = process.env.DATABASE_URL;
try {
  if (connString) {
    const url = new URL(connString);
    // 移除 sslmode/pgbouncer 等查询参数，避免覆盖 ssl 配置
    url.searchParams.delete('sslmode');
    connString = url.toString();
  }
} catch (_) {}

const pool = new Pool({
  connectionString: connString,
  ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false },
  max: parseInt(process.env.DB_POOL_MAX || '20', 10),
  idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE || '30000', 10),
  connectionTimeoutMillis: parseInt(process.env.DB_POOL_CONN_TIMEOUT || '5000', 10),
});

async function ensureSchema() {
  await pool.query(`
    create table if not exists users (
      id serial primary key,
      phone_code varchar(8) not null,
      phone varchar(32) not null,
      password_hash text not null,
      name varchar(100) not null,
      avatar_url text,
      age int,
      gender varchar(16),
      zodiac varchar(32),
      nationality varchar(64),
      region varchar(128),
      personality text,
      hobby text,
      match_enabled boolean default false,
      photos text[],
      created_at timestamptz default now(),
      unique(phone_code, phone),
      unique(name)
    );
    alter table users add column if not exists avatar_url text;
    alter table users add column if not exists age int;
    alter table users add column if not exists gender varchar(16);
    alter table users add column if not exists zodiac varchar(32);
    alter table users add column if not exists nationality varchar(64);
    alter table users add column if not exists region varchar(128);
    alter table users add column if not exists personality text;
    alter table users add column if not exists hobby text;
    alter table users add column if not exists match_enabled boolean default false;
    alter table users alter column match_enabled set default false;
    alter table users add column if not exists photos text[];
    alter table users add column if not exists is_guest boolean default false;
    alter table users add column if not exists guest_token text;
    alter table users add column if not exists guest_expired_at timestamptz;
    alter table users alter column created_at set default now();
    with dupes as (
      select id, name, row_number() over (partition by name order by id) as rn
      from users
      where name is not null and name <> ''
    )
    update users u
    set name = u.name || '_' || u.id
    from dupes
    where u.id = dupes.id and dupes.rn > 1;
    create unique index if not exists users_name_key on users(name);
    create unique index if not exists users_guest_token_key on users(guest_token);
    create table if not exists matches (
      id serial primary key,
      user_id int,
      matched_user_id int,
      created_at timestamptz default now()
    );
    create table if not exists match_cursor (
      id int primary key,
      last_user_id int,
      updated_at timestamptz default now()
    );
    insert into match_cursor (id, last_user_id)
    values (1, 0)
    on conflict (id) do nothing;
    create table if not exists chats (
      id serial primary key,
      created_at timestamptz default now(),
      last_message_at timestamptz,
      last_message text,
      last_message_image text,
      guest_expired boolean default false,
      guest_expired_at timestamptz
    );
    alter table chats add column if not exists last_message_at timestamptz;
    alter table chats add column if not exists last_message text;
    alter table chats add column if not exists last_message_image text;
    alter table chats add column if not exists guest_expired boolean default false;
    alter table chats add column if not exists guest_expired_at timestamptz;
    create table if not exists chat_participants (
      chat_id int not null,
      user_id int not null,
      created_at timestamptz default now(),
      unread_count int default 0,
      primary key (chat_id, user_id)
    );
    alter table chat_participants add column if not exists unread_count int default 0;
    create table if not exists messages (
      id serial primary key,
      chat_id int not null,
      sender_id int not null,
      content text not null,
      image_url text,
      created_at timestamptz default now()
    );
    alter table messages add column if not exists image_url text;
    alter table messages add column if not exists read_at timestamptz;
    create index if not exists messages_chat_id_idx on messages(chat_id, created_at);
    create index if not exists messages_unread_idx on messages(chat_id, read_at);
    create index if not exists messages_chat_id_id_idx on messages(chat_id, id);
    create index if not exists messages_chat_sender_read_idx on messages(chat_id, sender_id, read_at);
    create index if not exists chat_participants_user_idx on chat_participants(user_id, chat_id);
    create index if not exists chats_last_message_idx on chats(last_message_at desc, id desc);
    update chat_participants cp
    set unread_count = coalesce(sub.cnt, 0)
    from (
      select cp2.chat_id, cp2.user_id, count(*)::int as cnt
      from chat_participants cp2
      join messages m on m.chat_id = cp2.chat_id
      where m.sender_id <> cp2.user_id and m.read_at is null
      group by cp2.chat_id, cp2.user_id
    ) sub
    where cp.chat_id = sub.chat_id and cp.user_id = sub.user_id;
  `);
}

module.exports = { pool, ensureSchema };
