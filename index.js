require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
let createAdapter;
let createClient;
try {
  ({ createAdapter } = require('@socket.io/redis-adapter'));
} catch (_) {
  createAdapter = null;
}
try {
  ({ createClient } = require('redis'));
} catch (_) {
  createClient = null;
}
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const crypto = require('crypto');
const { supabase } = require('./supabase');
const { pool, ensureSchema } = require('./db');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret-change-me';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
let adminTokenWarned = false;
let io;
const guestCache = new Map();
const GUEST_CACHE_TTL = 10 * 60 * 1000;
const messageQueues = new Map();
const processingChats = new Set();
let writeInFlight = 0;
const writeWaiters = [];
const MAX_WRITE_CONCURRENCY = parseInt(process.env.WRITE_CONCURRENCY || '8', 10);
const QUEUE_BATCH_SIZE = parseInt(process.env.WRITE_BATCH || '10', 10);
const CHAT_LIST_CACHE_TTL = 2000;
const MESSAGES_CACHE_TTL = 2000;
const MAX_RECENT_MESSAGES = 50;
const CHAT_PARTICIPANTS_TTL = 5 * 60 * 1000;
const CHAT_STATUS_TTL = 10 * 1000;
const chatListCache = new Map();
const messagesCache = new Map();
const chatParticipantsCache = new Map();
const chatStatusCache = new Map();
const onlineUsers = new Map();
const lastSeenMap = new Map();

function getCachedEntry(map, key) {
  const hit = map.get(key);
  if (!hit) return null;
  if (hit.expiresAt <= Date.now()) {
    map.delete(key);
    return null;
  }
  return hit;
}

function setCacheEntry(map, key, value, ttl) {
  map.set(key, { value, expiresAt: Date.now() + ttl });
}

function invalidateChatListCache(userIds) {
  if (!Array.isArray(userIds)) return;
  userIds.forEach((uid) => chatListCache.delete(String(uid)));
}

function sortChatsByTime(list) {
  return [...list].sort((a, b) => {
    const at = a.last_message_at ? new Date(a.last_message_at).getTime() : 0;
    const bt = b.last_message_at ? new Date(b.last_message_at).getTime() : 0;
    if (bt !== at) return bt - at;
    return b.id - a.id;
  });
}

function updateChatListCache(userIds, chatId, message, senderId) {
  if (!Array.isArray(userIds)) return;
  const lastMessage = message?.content || (message?.image_url ? '[图片]' : '');
  const lastTime = message?.created_at || new Date().toISOString();
  userIds.forEach((uid) => {
    const hit = getCachedEntry(chatListCache, String(uid));
    if (!hit) return;
    const data = hit.value;
    if (!data || !Array.isArray(data.chats)) return;
    const nextChats = data.chats.map((c) => {
      if (c.id !== chatId) return c;
      const unread = c.unread_count || 0;
      return {
        ...c,
        last_message: lastMessage,
        last_message_at: lastTime,
        unread_count: uid === senderId ? 0 : unread + 1,
      };
    });
    setCacheEntry(
      chatListCache,
      String(uid),
      { ...data, chats: sortChatsByTime(nextChats) },
      CHAT_LIST_CACHE_TTL
    );
  });
}

function markChatListRead(userId, chatId) {
  const hit = getCachedEntry(chatListCache, String(userId));
  if (!hit) return;
  const data = hit.value;
  if (!data || !Array.isArray(data.chats)) return;
  const nextChats = data.chats.map((c) => (c.id === chatId ? { ...c, unread_count: 0 } : c));
  setCacheEntry(chatListCache, String(userId), { ...data, chats: nextChats }, CHAT_LIST_CACHE_TTL);
}

function getMessagesCache(chatId) {
  return getCachedEntry(messagesCache, String(chatId));
}

function setMessagesCache(chatId, payload) {
  setCacheEntry(messagesCache, String(chatId), payload, MESSAGES_CACHE_TTL);
}

function appendMessagesCache(chatId, newMessages) {
  if (!Array.isArray(newMessages) || newMessages.length === 0) return;
  const hit = getMessagesCache(chatId);
  if (!hit) return;
  const existing = hit.value?.messages || [];
  const merged = [...existing, ...newMessages].sort((a, b) => a.id - b.id);
  const trimmed = merged.slice(-MAX_RECENT_MESSAGES);
  setMessagesCache(chatId, { ...hit.value, messages: trimmed });
}

async function getChatParticipants(chatId) {
  const hit = getCachedEntry(chatParticipantsCache, String(chatId));
  if (hit) return hit.value;
  const { rows } = await pool.query('select user_id from chat_participants where chat_id = $1', [chatId]);
  const ids = rows.map((r) => r.user_id);
  setCacheEntry(chatParticipantsCache, String(chatId), ids, CHAT_PARTICIPANTS_TTL);
  return ids;
}

async function getChatStatus(chatId) {
  const hit = getCachedEntry(chatStatusCache, String(chatId));
  if (hit) return hit.value;
  const { rows } = await pool.query('select coalesce(guest_expired, false) as guest_expired from chats where id = $1', [chatId]);
  const status = { guest_expired: rows[0]?.guest_expired ?? false };
  setCacheEntry(chatStatusCache, String(chatId), status, CHAT_STATUS_TTL);
  return status;
}

function setChatStatusCache(chatId, status) {
  setCacheEntry(chatStatusCache, String(chatId), status, CHAT_STATUS_TTL);
}

function isUserOnline(userId) {
  return onlineUsers.has(userId);
}

function touchOnline(userId) {
  const count = (onlineUsers.get(userId) || 0) + 1;
  onlineUsers.set(userId, count);
  return count;
}

function touchOffline(userId) {
  const count = (onlineUsers.get(userId) || 0) - 1;
  if (count <= 0) {
    onlineUsers.delete(userId);
    lastSeenMap.set(userId, Date.now());
    return 0;
  }
  onlineUsers.set(userId, count);
  return count;
}

function emitPresence(userId, online) {
  if (!io || !userId) return;
  io.to(`presence:${userId}`).emit('presence:update', {
    user_id: userId,
    online,
    last_seen: online ? null : lastSeenMap.get(userId) || null,
  });
}

function acquireWriteSlot() {
  if (writeInFlight < MAX_WRITE_CONCURRENCY) {
    writeInFlight += 1;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    writeWaiters.push(resolve);
  });
}

function releaseWriteSlot() {
  writeInFlight = Math.max(0, writeInFlight - 1);
  const next = writeWaiters.shift();
  if (next) {
    writeInFlight += 1;
    next();
  }
}

function enqueueMessageJob(job) {
  if (!messageQueues.has(job.chatId)) {
    messageQueues.set(job.chatId, []);
  }
  messageQueues.get(job.chatId).push(job);
  if (!processingChats.has(job.chatId)) {
    processChatQueue(job.chatId);
  }
}

async function processChatQueue(chatId) {
  const queue = messageQueues.get(chatId);
  if (!queue || queue.length === 0) {
    messageQueues.delete(chatId);
    processingChats.delete(chatId);
    return;
  }
  processingChats.add(chatId);
  const batch = [];
  const firstJob = queue.shift();
  if (!firstJob) {
    processingChats.delete(chatId);
    return;
  }
  batch.push(firstJob);
  while (queue.length && batch.length < QUEUE_BATCH_SIZE) {
    const nextJob = queue.shift();
    if (!nextJob) break;
    batch.push(nextJob);
  }
  if (!Number.isFinite(chatId)) {
    console.warn('[queue] invalid chatId', chatId);
    batch.forEach((job) => {
      if (!job?.senderId) return;
      emitToUsers([job.senderId], 'message:failed', { chat_id: chatId, client_id: job.clientId });
    });
    if (queue.length === 0) {
      messageQueues.delete(chatId);
      processingChats.delete(chatId);
    } else {
      processChatQueue(chatId);
    }
    return;
  }
  await acquireWriteSlot();
  try {
    const participantIds = batch[0]?.participantIds || (await getChatParticipants(chatId));
    const values = [];
    const params = [];
    let idx = 1;
    batch.forEach((job) => {
      values.push(`($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3})`);
      params.push(chatId, job.senderId, job.content, null);
      idx += 4;
    });
    const { rows } = await pool.query(
      `insert into messages (chat_id, sender_id, content, image_url)
       values ${values.join(', ')}
       returning id, sender_id, content, image_url, created_at, read_at`,
      params
    );
    const message = rows[rows.length - 1];
    await pool.query(
      'update chats set last_message_at = $1, last_message = $2, last_message_image = $3 where id = $4',
      [message.created_at, message.content, message.image_url, chatId]
    );
    const unreadIncrements = new Map();
    batch.forEach((job) => {
      participantIds.forEach((uid) => {
        if (uid === job.senderId) return;
        unreadIncrements.set(uid, (unreadIncrements.get(uid) || 0) + 1);
      });
    });
    if (unreadIncrements.size) {
      const entries = Array.from(unreadIncrements.entries());
      const userIds = entries
        .map(([uid]) => Number(uid))
        .filter((uid) => Number.isFinite(uid));
      const incs = entries
        .map(([, inc]) => Number(inc))
        .filter((inc) => Number.isFinite(inc));
      if (userIds.length !== incs.length || userIds.length === 0) {
        console.warn('[queue] invalid unread increment entries', entries);
      } else {
      await pool.query(
        `update chat_participants as cp
         set unread_count = cp.unread_count + v.inc
         from (
           select unnest($1::int[]) as user_id, unnest($2::int[]) as inc
         ) as v
         where cp.chat_id = $3
           and cp.user_id = v.user_id`,
        [userIds, incs, chatId]
      );
      }
    }
    rows.forEach((row, i) => {
      const job = batch[i];
      emitToUsers(participantIds, 'message:ack', { chat_id: chatId, client_id: job.clientId, message: row });
      updateChatListCache(participantIds, chatId, row, row.sender_id);
    });
    appendMessagesCache(chatId, rows);
  } catch (err) {
    console.error('[queue] write failed', err);
    batch.forEach((job) => {
      if (!job?.senderId) return;
      emitToUsers([job.senderId], 'message:failed', { chat_id: chatId, client_id: job.clientId });
    });
  } finally {
    releaseWriteSlot();
    if (queue.length === 0) {
      messageQueues.delete(chatId);
      processingChats.delete(chatId);
    } else {
      processChatQueue(chatId);
    }
  }
}

function getGuestFromCache(token) {
  const hit = guestCache.get(token);
  if (!hit) return null;
  if (hit.expiresAt <= Date.now()) {
    guestCache.delete(token);
    return null;
  }
  return hit.id;
}

function setGuestCache(token, id) {
  if (!token || !id) return;
  guestCache.set(token, { id, expiresAt: Date.now() + GUEST_CACHE_TTL });
}

function signToken(user) {
  return jwt.sign({ id: user.id, phone: user.phone, phoneCode: user.phone_code }, JWT_SECRET, { expiresIn: '7d' });
}

function authenticateJWT(req, res, next) {
  const auth = req.headers.authorization || '';
  const [, token] = auth.split(' ');
  if (!token) return res.status(401).json({ error: '未授权' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.user = payload;
    next();
  } catch (e) {
    return res.status(401).json({ error: '无效或过期的令牌' });
  }
}

function verifyToken(token) {
  if (!token) return null;
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (_) {
    return null;
  }
}

function getUserIdFromToken(req) {
  const auth = req.headers.authorization || '';
  const [, token] = auth.split(' ');
  if (!token) return null;
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    return payload?.id ?? null;
  } catch (_) {
    return null;
  }
}

function getGuestToken(req) {
  const raw = req.headers['x-guest-token'];
  if (typeof raw !== 'string') return null;
  const token = raw.trim();
  return token ? token : null;
}

async function ensureGuestUser(guestToken) {
  if (!guestToken) return null;
  const { rows } = await pool.query(
    'select id from users where guest_token = $1 and is_guest = true limit 1',
    [guestToken]
  );
  if (rows.length) return rows[0].id;
  const shortHash = crypto.createHash('sha256').update(guestToken).digest('hex');
  const phone = `g_${shortHash.slice(0, 29)}`; // <= 32 chars
  const passwordHash = await bcrypt.hash(`${guestToken}_${Date.now()}`, 8);
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const rand4 = Math.random().toString(36).slice(2, 6).toUpperCase();
    const name = `游客${rand4}`;
    try {
      const inserted = await pool.query(
        `insert into users (phone_code, phone, password_hash, name, is_guest, guest_token)
         values ($1, $2, $3, $4, true, $5)
         on conflict (phone_code, phone) do nothing
         returning id`,
        ['+00', phone, passwordHash, name, guestToken]
      );
      if (inserted.rows.length) return inserted.rows[0].id;
      break;
    } catch (err) {
      if (err && err.code === '23505' && err.constraint === 'users_name_key') {
        continue;
      }
      throw err;
    }
  }
  const existing = await pool.query(
    'select id from users where phone_code = $1 and phone = $2 limit 1',
    ['+00', phone]
  );
  return existing.rows[0]?.id ?? null;
}

async function resolveAuth(req) {
  const userId = getUserIdFromToken(req);
  if (userId) return { userId, isGuest: false, guestToken: null };
  const guestToken = getGuestToken(req);
  if (!guestToken) return { userId: null, isGuest: false, guestToken: null };
  const cached = getGuestFromCache(guestToken);
  if (cached) return { userId: cached, isGuest: true, guestToken };
  const guestUserId = await ensureGuestUser(guestToken);
  if (guestUserId) setGuestCache(guestToken, guestUserId);
  return { userId: guestUserId, isGuest: true, guestToken };
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    if (!adminTokenWarned) {
      console.warn('[admin] ADMIN_TOKEN 未配置，管理员接口将不做鉴权保护');
      adminTokenWarned = true;
    }
    return next();
  }
  const headerToken = req.headers['x-admin-token'];
  const auth = req.headers.authorization || '';
  const bearer = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const token = (typeof headerToken === 'string' ? headerToken : '') || bearer;
  if (!token || token !== ADMIN_TOKEN) {
    return res.status(403).json({ error: '管理员权限不足' });
  }
  return next();
}

function phoneCodeToNation(code = '+86') {
  const map = {
    '+86': { flag: '🇨🇳', name: '中国' },
    '+852': { flag: '🇭🇰', name: '中国香港' },
    '+853': { flag: '🇲🇴', name: '中国澳门' },
    '+886': { flag: '🇹🇼', name: '中国台湾' },
    '+81': { flag: '🇯🇵', name: '日本' },
    '+82': { flag: '🇰🇷', name: '韩国' },
    '+1': { flag: '🇺🇸', name: '美国' },
    '+44': { flag: '🇬🇧', name: '英国' },
    '+33': { flag: '🇫🇷', name: '法国' },
    '+49': { flag: '🇩🇪', name: '德国' },
    '+60': { flag: '🇲🇾', name: '马来西亚' },
    '+65': { flag: '🇸🇬', name: '新加坡' },
  };
  const hit = map[code];
  return hit ? `${hit.flag} ${hit.name}` : '🌐 未知国家';
}

function toPublicUrl(req, raw) {
  if (!raw) return null;
  if (raw.startsWith('http')) return raw;
  if (raw.startsWith('/uploads/')) return `${req.protocol}://${req.get('host')}${raw}`;
  if (supabase) {
    const bucket = process.env.SUPABASE_BUCKET || 'avatars';
    const { data } = supabase.storage.from(bucket).getPublicUrl(raw);
    return data?.publicUrl || raw;
  }
  return raw;
}

function emitToUsers(userIds, event, payload) {
  if (!io || !Array.isArray(userIds)) return;
  userIds.forEach((uid) => {
    if (!uid) return;
    io.to(`user:${uid}`).emit(event, payload);
  });
}

// ensure uploads dir (for legacy local files cleanup)
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
const avatarDir = path.join(uploadsDir, 'avatars');
if (!fs.existsSync(avatarDir)) fs.mkdirSync(avatarDir, { recursive: true });

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 2 * 1024 * 1024 }, // 2MB
  fileFilter: (_req, file, cb) => {
    if (!file.mimetype.startsWith('image/')) return cb(new Error('只允许上传图片'));
    cb(null, true);
  },
});

function sniffImageExt(buf, originalName = '') {
  const isPng = buf.slice(0, 8).toString('hex') === '89504e470d0a1a0a';
  const isJpg = buf[0] === 0xff && buf[1] === 0xd8 && buf[buf.length - 2] === 0xff && buf[buf.length - 1] === 0xd9;
  const isGif = buf.slice(0, 6).toString() === 'GIF87a' || buf.slice(0, 6).toString() === 'GIF89a';
  const isWebp = buf.slice(0, 4).toString() === 'RIFF' && buf.slice(8, 12).toString() === 'WEBP';
  if (!(isPng || isJpg || isGif || isWebp)) return null;
  return path.extname(originalName || '').toLowerCase() || (isPng ? '.png' : isJpg ? '.jpg' : isGif ? '.gif' : '.webp');
}

// Match (public or authed)
app.get('/api/match', async (req, res) => {
  try {
    const { userId: requesterId, isGuest } = await resolveAuth(req);
    if (!requesterId && !getGuestToken(req)) {
      return res.status(401).json({ error: '请先登录或继续以游客身份' });
    }
    if (!requesterId) {
      return res.status(401).json({ error: '请先登录或继续以游客身份' });
    }
    if (isGuest) {
      try {
        const exists = await pool.query("select to_regclass('public.matches') as name");
        if (exists.rows[0]?.name) {
          const countRes = await pool.query(
            'select count(*)::int as count from matches where user_id = $1',
            [requesterId]
          );
          if ((countRes.rows[0]?.count ?? 0) >= 1) {
            return res.status(429).json({ error: '游客仅允许匹配一次' });
          }
        }
      } catch (_) {}
    }
    const client = await pool.connect();
    let user;
    try {
      await client.query("begin");
      const cursorRes = await client.query("select last_user_id from match_cursor where id = 1 for update");
      const lastUserId = cursorRes.rows[0]?.last_user_id ?? 0;
      const baseSql = `
        select u.id, u.name, u.avatar_url, u.zodiac, u.hobby, u.photos
        from users u
        where coalesce(u.match_enabled, false) = true
          and coalesce(u.is_guest, false) = false
          and u.id <> $1
          and not exists (
            select 1
            from chat_participants p1
            join chat_participants p2 on p1.chat_id = p2.chat_id
            where p1.user_id = $1 and p2.user_id = u.id
          )
          %EXTRA%
        order by u.id asc
        limit 1
      `;
      let { rows } = await client.query(baseSql.replace('%EXTRA%', 'and u.id > $2'), [requesterId, lastUserId]);
      if (!rows.length) {
        rows = (await client.query(baseSql.replace('%EXTRA%', ''), [requesterId])).rows;
      }
      if (!rows.length) {
        await client.query("rollback");
        return res.status(404).json({ error: '暂无可匹配用户' });
      }
      user = rows[0];
      await client.query("update match_cursor set last_user_id = $1, updated_at = now() where id = 1", [user.id]);
      await client.query("commit");
    } catch (err) {
      try {
        await client.query("rollback");
      } catch (_) {}
      throw err;
    } finally {
      client.release();
    }
    const photos = Array.isArray(user.photos)
      ? user.photos.map((p) => toPublicUrl(req, p)).filter(Boolean)
      : [];
    const avatar = toPublicUrl(req, user.avatar_url) || photos[0] || null;

    try {
      const exists = await pool.query("select to_regclass('public.matches') as name");
      if (exists.rows[0]?.name) {
        await pool.query(
          'insert into matches (user_id, matched_user_id) values ($1, $2)',
          [requesterId || null, user.id]
        );
      }
    } catch (_) {}

    res.json({
      user: {
        id: user.id,
        name: user.name,
        avatar_url: avatar,
        zodiac: user.zodiac,
        hobby: user.hobby,
        photos,
      },
      status: 'online',
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '匹配失败' });
  }
});

// Chats (auth or guest)
app.post('/api/chats', async (req, res) => {
  const { target_user_id, auto_hello, allow_self } = req.body || {};
  const targetId = parseInt(target_user_id, 10);
  const { userId, isGuest } = await resolveAuth(req);
  if (!Number.isFinite(targetId)) return res.status(400).json({ error: '目标用户无效' });
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (targetId === userId && !allow_self) return res.status(400).json({ error: '不能与自己对话' });
  try {
    const targetRes = await pool.query('select is_guest from users where id = $1', [targetId]);
    if (targetRes.rows[0]?.is_guest) {
      return res.status(400).json({ error: '目标用户不可匹配' });
    }
    const buildChatPreview = async (chatId) => {
      const { rows } = await pool.query(
        `select c.id,
                c.last_message_at,
                c.last_message,
                c.last_message_image,
                coalesce(c.guest_expired, false) as guest_expired,
                u.id as partner_id,
                u.name as partner_name,
                u.avatar_url as partner_avatar
         from chats c
         join chat_participants p1 on p1.chat_id = c.id and p1.user_id = $2
         left join chat_participants p2 on p2.chat_id = c.id and p2.user_id <> $2
         join users u on u.id = coalesce(p2.user_id, p1.user_id)
         where c.id = $1
         limit 1`,
        [chatId, userId]
      );
      if (!rows.length) return null;
      const row = rows[0];
      return {
        id: row.id,
        partner_id: row.partner_id,
        partner_name: row.partner_name,
        partner_avatar: toPublicUrl(req, row.partner_avatar),
        last_message: row.last_message || (row.last_message_image ? '[图片]' : ''),
        last_message_at: row.last_message_at,
        guest_expired: row.guest_expired,
        unread_count: 0,
      };
    };

    if (isGuest) {
      const countRes = await pool.query(
        'select count(*)::int as count from chat_participants where user_id = $1',
        [userId]
      );
      if ((countRes.rows[0]?.count ?? 0) >= 1) {
        return res.status(429).json({ error: '游客仅允许发起一次对话' });
      }
    }
    const existing = targetId === userId
      ? await pool.query(
          `select c.id
           from chats c
           join chat_participants p1 on p1.chat_id = c.id and p1.user_id = $1
           left join chat_participants p2 on p2.chat_id = c.id and p2.user_id <> $1
           where p2.user_id is null
           limit 1`,
          [userId]
        )
      : await pool.query(
          `select c.id
           from chats c
           join chat_participants p1 on p1.chat_id = c.id and p1.user_id = $1
           join chat_participants p2 on p2.chat_id = c.id and p2.user_id = $2
           limit 1`,
          [userId, targetId]
        );
    if (existing.rows.length) {
      const chatId = existing.rows[0].id;
      const chat = await buildChatPreview(chatId);
      return res.json({ chat_id: chatId, created: false, chat });
    }
    const chatRes = await pool.query('insert into chats default values returning id');
    const chatId = chatRes.rows[0].id;
    if (targetId === userId) {
      await pool.query(
        'insert into chat_participants (chat_id, user_id) values ($1, $2)',
        [chatId, userId]
      );
    } else {
      await pool.query(
        'insert into chat_participants (chat_id, user_id) values ($1, $2), ($1, $3)',
        [chatId, userId, targetId]
      );
    }
    invalidateChatListCache([userId, targetId]);
    chatParticipantsCache.delete(String(chatId));
    chatStatusCache.delete(String(chatId));
    if (auto_hello) {
      const { rows } = await pool.query(
        'insert into messages (chat_id, sender_id, content, image_url) values ($1, $2, $3, $4) returning id, sender_id, content, image_url, created_at, read_at',
        [chatId, userId, 'Hello👋', null]
      );
      const msg = rows[0];
      await pool.query(
        'update chats set last_message_at = $1, last_message = $2, last_message_image = $3 where id = $4',
        [msg.created_at, msg.content, msg.image_url, chatId]
      );
      await pool.query(
        'update chat_participants set unread_count = unread_count + 1 where chat_id = $1 and user_id <> $2',
        [chatId, userId]
      );
      emitToUsers([userId, targetId], 'message:new', { chat_id: chatId, message: rows[0] });
      appendMessagesCache(chatId, [msg]);
      updateChatListCache([userId, targetId], chatId, msg, userId);
      const chat = await buildChatPreview(chatId);
      return res.json({ chat_id: chatId, created: true, chat, message: msg });
    }
    const chat = await buildChatPreview(chatId);
    res.json({ chat_id: chatId, created: true, chat });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '创建对话失败' });
  }
});

app.get('/api/chats', async (req, res) => {
  const { userId } = await resolveAuth(req);
  if (!userId) return res.status(401).json({ error: '未授权' });
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 30, 100);
    const cursor = typeof req.query.cursor === 'string' ? req.query.cursor : '';
    if (!cursor && limit <= 50) {
      const cached = getCachedEntry(chatListCache, String(userId));
      if (cached) {
        return res.json(cached.value);
      }
    }
    let cursorTime = null;
    let cursorId = null;
    if (cursor) {
      const [timeStr, idStr] = cursor.split('|');
      const t = new Date(timeStr);
      const idNum = parseInt(idStr, 10);
      if (!Number.isNaN(t.getTime()) && Number.isFinite(idNum)) {
        cursorTime = t.toISOString();
        cursorId = idNum;
      }
    }

    const params = [userId];
    let cursorClause = '';
    if (cursorTime && cursorId) {
      params.push(cursorTime, cursorId);
      cursorClause = `and (
        coalesce(c.last_message_at, c.created_at) < $2
        or (coalesce(c.last_message_at, c.created_at) = $2 and c.id < $3)
      )`;
    }
    params.push(limit + 1);
    const limitIdx = params.length;

    const { rows } = await pool.query(
      `with my_chats as (
         select c.*
         from chats c
         join chat_participants p on p.chat_id = c.id and p.user_id = $1
         where 1=1
         ${cursorClause}
         order by coalesce(c.last_message_at, c.created_at) desc, c.id desc
         limit $${limitIdx}
       )
       select mc.id,
         mc.created_at,
         coalesce(mc.last_message_at, mc.created_at) as sort_time,
         mc.last_message_at,
         mc.last_message,
         mc.last_message_image,
         coalesce(mc.guest_expired, false) as guest_expired,
         u.id as partner_id,
         u.name as partner_name,
         u.avatar_url as partner_avatar,
         coalesce(p1.unread_count, 0) as unread_count
       from my_chats mc
       join chat_participants p1 on p1.chat_id = mc.id and p1.user_id = $1
       left join chat_participants p2 on p2.chat_id = mc.id and p2.user_id <> $1
       join users u on u.id = coalesce(p2.user_id, p1.user_id)
       order by sort_time desc, mc.id desc`,
      params
    );

    const hasMore = rows.length > limit;
    const sliced = hasMore ? rows.slice(0, limit) : rows;
    const chats = sliced.map((r) => ({
      id: r.id,
      partner_id: r.partner_id,
      partner_name: r.partner_name,
      partner_avatar: toPublicUrl(req, r.partner_avatar),
      last_message: r.last_message || (r.last_message_image ? '[图片]' : ''),
      last_message_at: r.last_message_at || r.sort_time,
      guest_expired: r.guest_expired ?? false,
      unread_count: r.unread_count ?? 0,
    }));
    let nextCursor = null;
    if (hasMore && sliced.length) {
      const last = sliced[sliced.length - 1];
      const sortTime = last.sort_time ? new Date(last.sort_time).toISOString() : new Date().toISOString();
      nextCursor = `${sortTime}|${last.id}`;
    }
    const payload = { chats, next_cursor: nextCursor, has_more: hasMore };
    if (!cursor && limit <= 50) {
      setCacheEntry(chatListCache, String(userId), payload, CHAT_LIST_CACHE_TTL);
    }
    res.json(payload);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取对话失败' });
  }
});

app.get('/api/chats/:id/messages', async (req, res) => {
  const { userId } = await resolveAuth(req);
  const chatId = parseInt(req.params.id, 10);
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (!Number.isFinite(chatId)) return res.status(400).json({ error: '对话ID无效' });
  try {
    const participantIds = await getChatParticipants(chatId);
    if (!participantIds.includes(userId)) return res.status(403).json({ error: '无权限' });
    const limit = Math.min(parseInt(req.query.limit, 10) || 30, 200);
    const beforeId = parseInt(req.query.before_id, 10);
    const before = Number.isFinite(beforeId) ? beforeId : null;
    if (!before && limit <= MAX_RECENT_MESSAGES) {
      const cached = getMessagesCache(chatId);
      if (cached) {
        const messages = Array.isArray(cached.value?.messages) ? cached.value.messages.slice(-limit) : [];
        const nextBeforeId = cached.value?.next_before_id || null;
        const markRead = req.query.mark_read !== '0';
        if (markRead) {
          Promise.resolve()
            .then(async () => {
              await pool.query(
                'update messages set read_at = now() where chat_id = $1 and sender_id <> $2 and read_at is null',
                [chatId, userId]
              );
              await pool.query(
                'update chat_participants set unread_count = 0 where chat_id = $1 and user_id = $2',
                [chatId, userId]
              );
              emitToUsers(participantIds, 'messages:read', { chat_id: chatId, reader_id: userId });
              markChatListRead(userId, chatId);
            })
            .catch(() => {});
        }
        return res.json({ messages, next_before_id: nextBeforeId });
      }
    }
    const params = before ? [chatId, before, limit + 1] : [chatId, limit + 1];
    const where = before ? 'chat_id = $1 and id < $2' : 'chat_id = $1';
    const { rows } = await pool.query(
      `select id, sender_id, content, image_url, created_at, read_at
       from messages
       where ${where}
       order by id desc
       limit $${params.length}`,
      params
    );
    const hasMore = rows.length > limit;
    const sliced = hasMore ? rows.slice(0, limit) : rows;
    const ordered = sliced.reverse();
    const markRead = req.query.mark_read !== '0';
    if (markRead) {
      Promise.resolve()
        .then(async () => {
          await pool.query(
            'update messages set read_at = now() where chat_id = $1 and sender_id <> $2 and read_at is null',
            [chatId, userId]
          );
          await pool.query(
            'update chat_participants set unread_count = 0 where chat_id = $1 and user_id = $2',
            [chatId, userId]
          );
          emitToUsers(participantIds, 'messages:read', { chat_id: chatId, reader_id: userId });
          markChatListRead(userId, chatId);
        })
        .catch(() => {});
    }
    const nextBeforeId = ordered.length ? ordered[0].id : null;
    if (!before && limit <= MAX_RECENT_MESSAGES) {
      setMessagesCache(chatId, { messages: ordered, next_before_id: hasMore ? nextBeforeId : null });
    }
    res.json({ messages: ordered, next_before_id: hasMore ? nextBeforeId : null });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取消息失败' });
  }
});

app.post('/api/chats/:id/read', async (req, res) => {
  const { userId } = await resolveAuth(req);
  const chatId = parseInt(req.params.id, 10);
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (!Number.isFinite(chatId)) return res.status(400).json({ error: '对话ID无效' });
  try {
    const allowed = await pool.query(
      'select 1 from chat_participants where chat_id = $1 and user_id = $2',
      [chatId, userId]
    );
    if (!allowed.rows.length) return res.status(403).json({ error: '无权限' });
    const result = await pool.query(
      'update messages set read_at = now() where chat_id = $1 and sender_id <> $2 and read_at is null',
      [chatId, userId]
    );
    await pool.query(
      'update chat_participants set unread_count = 0 where chat_id = $1 and user_id = $2',
      [chatId, userId]
    );
    const ids = await getChatParticipants(chatId);
    emitToUsers(ids, 'messages:read', { chat_id: chatId, reader_id: userId });
    markChatListRead(userId, chatId);
    res.json({ updated: result.rowCount || 0 });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '更新失败' });
  }
});

app.delete('/api/chats/:id', async (req, res) => {
  const { userId } = await resolveAuth(req);
  const chatId = parseInt(req.params.id, 10);
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (!Number.isFinite(chatId)) return res.status(400).json({ error: '对话ID无效' });
  try {
    const allowed = await pool.query(
      'select 1 from chat_participants where chat_id = $1 and user_id = $2',
      [chatId, userId]
    );
    if (!allowed.rows.length) return res.status(403).json({ error: '无权限' });
    const ids = await getChatParticipants(chatId);
    const status = await getChatStatus(chatId);
    if (!status?.guest_expired) {
      return res.status(403).json({ error: '仅可删除已过期对话' });
    }
    await pool.query('delete from messages where chat_id = $1', [chatId]);
    await pool.query('delete from chat_participants where chat_id = $1', [chatId]);
    await pool.query('delete from chats where id = $1', [chatId]);
    invalidateChatListCache(ids);
    messagesCache.delete(String(chatId));
    chatParticipantsCache.delete(String(chatId));
    chatStatusCache.delete(String(chatId));
    res.json({ deleted: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '删除失败' });
  }
});

app.post('/api/chats/:id/messages', async (req, res) => {
  const { userId } = await resolveAuth(req);
  const chatId = parseInt(req.params.id, 10);
  const { content, client_id } = req.body || {};
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (!Number.isFinite(chatId)) return res.status(400).json({ error: '对话ID无效' });
  if (!content || !String(content).trim()) return res.status(400).json({ error: '消息不能为空' });
  try {
    const ids = await getChatParticipants(chatId);
    if (!ids.includes(userId)) return res.status(403).json({ error: '无权限' });
    const status = await getChatStatus(chatId);
    if (status?.guest_expired) {
      return res.status(403).json({ error: '对话已过期' });
    }
    const trimmed = String(content).trim();
    const clientId = typeof client_id === 'string' && client_id ? client_id : `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const now = new Date().toISOString();
    emitToUsers(ids, 'message:new', {
      chat_id: chatId,
      message: {
        id: clientId,
        client_id: clientId,
        sender_id: userId,
        content: trimmed,
        image_url: null,
        created_at: now,
        read_at: null,
        pending: true,
      },
    });
    enqueueMessageJob({
      chatId,
      senderId: userId,
      content: trimmed,
      clientId,
      participantIds: ids,
    });
    res.json({ queued: true, client_id: clientId });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '发送失败' });
  }
});

app.post('/api/chats/:id/images', upload.single('image'), async (req, res) => {
  const { userId } = await resolveAuth(req);
  const chatId = parseInt(req.params.id, 10);
  if (!userId) return res.status(401).json({ error: '未授权' });
  if (!Number.isFinite(chatId)) return res.status(400).json({ error: '对话ID无效' });
  try {
    const ids = await getChatParticipants(chatId);
    if (!ids.includes(userId)) return res.status(403).json({ error: '无权限' });
    const status = await getChatStatus(chatId);
    if (status?.guest_expired) {
      return res.status(403).json({ error: '对话已过期' });
    }
    if (!req.file) return res.status(400).json({ error: '未收到文件' });
    if (!supabase) return res.status(500).json({ error: '存储服务未配置' });

    const buf = req.file.buffer;
    const ext = sniffImageExt(buf, req.file.originalname);
    if (!ext) return res.status(400).json({ error: '文件格式不支持' });
    const bucket = process.env.SUPABASE_BUCKET || 'avatars';
    const objectKey = `chat_images/${chatId}/${Date.now()}_${Math.random().toString(36).slice(2, 8)}${ext}`;

    const { error: uploadErr } = await supabase.storage
      .from(bucket)
      .upload(objectKey, buf, { contentType: req.file.mimetype, upsert: true });
    if (uploadErr) throw uploadErr;

    const { data: pub } = supabase.storage.from(bucket).getPublicUrl(objectKey);
    const publicUrl = pub?.publicUrl || objectKey;

    const { rows } = await pool.query(
      'insert into messages (chat_id, sender_id, content, image_url) values ($1, $2, $3, $4) returning id, sender_id, content, image_url, created_at, read_at',
      [chatId, userId, '', publicUrl]
    );
    const message = rows[0];
    await pool.query(
      'update chats set last_message_at = $1, last_message = $2, last_message_image = $3 where id = $4',
      [message.created_at, message.content, message.image_url, chatId]
    );
    emitToUsers(ids, 'message:new', { chat_id: chatId, message });
    appendMessagesCache(chatId, [message]);
    updateChatListCache(ids, chatId, message, userId);
    res.json({ message });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '发送图片失败' });
  }
});

// Health
app.get('/health', (_, res) => res.json({ ok: true }));

// Register
app.post('/api/register', async (req, res) => {
  const {
    phoneCode = '+86',
    phone,
    password,
    name,
    avatarUrl,
    age,
    gender,
    zodiac,
    nationality,
    region,
    personality,
    hobby,
    photos,
    guest_token,
  } = req.body || {};
  if (!phone || !password || !name) {
    return res.status(400).json({ error: '缺少必填项: phone, password, name' });
  }
  try {
    const hash = await bcrypt.hash(password, 10);
    const insertSql = `
      insert into users (phone_code, phone, password_hash, name, avatar_url, age, gender, zodiac, nationality, region, personality, hobby, photos, match_enabled)
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      on conflict (phone_code, phone) do nothing
      returning id, phone_code, phone, name, avatar_url, age, gender, zodiac, nationality, region, personality, hobby, photos, created_at;
    `;
    const photosArr = Array.isArray(photos) ? photos.slice(0, 5) : null;
    const derivedNation = phoneCodeToNation(phoneCode);
    const nationToSave = nationality && nationality.trim() ? nationality : derivedNation;
    const guestToken = typeof guest_token === 'string' ? guest_token.trim() : '';
    if (guestToken) {
      const guestRes = await pool.query(
        'select id from users where guest_token = $1 and is_guest = true limit 1',
        [guestToken]
      );
      if (guestRes.rows.length) {
        try {
          const updateSql = `
            update users
            set phone_code = $1,
                phone = $2,
                password_hash = $3,
                name = $4,
                avatar_url = $5,
                age = $6,
                gender = $7,
                zodiac = $8,
                nationality = $9,
                region = $10,
                personality = $11,
                hobby = $12,
                photos = $13,
                match_enabled = false,
                is_guest = false,
                guest_token = null
            where id = $14
            returning id, phone_code, phone, name, avatar_url, age, gender, zodiac, nationality, region, personality, hobby, photos, created_at;
          `;
          const updateRes = await pool.query(updateSql, [
            phoneCode,
            phone,
            hash,
            name,
            avatarUrl || null,
            age ?? null,
            gender ?? null,
            zodiac ?? null,
            nationToSave ?? null,
            region ?? null,
            personality ?? null,
            hobby ?? null,
            photosArr,
            guestRes.rows[0].id,
          ]);
          const user = updateRes.rows[0];
          const token = signToken(user);
          return res.json({ token, user });
        } catch (err) {
          if (err.code === '23505') {
            return res.status(409).json({ error: '昵称或手机号已被占用' });
          }
          throw err;
        }
      }
    }
    const result = await pool.query(insertSql, [
      phoneCode,
      phone,
      hash,
      name,
      avatarUrl || null,
      age ?? null,
      gender ?? null,
      zodiac ?? null,
      nationToSave ?? null,
      region ?? null,
      personality ?? null,
      hobby ?? null,
      photosArr,
      false,
    ]);
    if (result.rowCount === 0) {
      return res.status(409).json({ error: '该手机号已注册' });
    }
    const user = result.rows[0];
    const token = signToken(user);
    res.json({ token, user });
  } catch (err) {
    console.error(err);
    if (err.code === '23505') {
      return res.status(409).json({ error: '昵称已被占用' });
    }
    res.status(500).json({ error: '注册失败' });
  }
});

// Login
app.post('/api/login', async (req, res) => {
  const { phoneCode = '+86', phone, password } = req.body || {};
  if (!phone || !password) {
    return res.status(400).json({ error: '缺少必填项: phone, password' });
  }
  try {
    const { rows } = await pool.query('select * from users where phone_code = $1 and phone = $2 limit 1', [phoneCode, phone]);
    if (!rows.length) return res.status(401).json({ error: '用户不存在或密码错误' });
    const user = rows[0];
    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) return res.status(401).json({ error: '用户不存在或密码错误' });
    const token = signToken(user);
    res.json({ token, user: { id: user.id, phone_code: user.phone_code, phone: user.phone, name: user.name, avatar_url: user.avatar_url, created_at: user.created_at } });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '登录失败' });
  }
});

// Guest profile
app.get('/api/guest/me', async (req, res) => {
  try {
    const guestToken = getGuestToken(req);
    if (!guestToken) return res.status(401).json({ error: '未授权' });
    const guestUserId = await ensureGuestUser(guestToken);
    if (!guestUserId) return res.status(401).json({ error: '未授权' });
    const { rows } = await pool.query(
      'select id, name, avatar_url, created_at from users where id = $1',
      [guestUserId]
    );
    if (!rows.length) return res.status(404).json({ error: '用户不存在' });
    const user = rows[0];
    res.json({ ...user, avatar_url: toPublicUrl(req, user.avatar_url) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取用户信息失败' });
  }
});

// 获取个人信息
app.get('/api/me', authenticateJWT, async (req, res) => {
  try {
    const { rows } = await pool.query('select id, phone_code, phone, name, avatar_url, age, gender, zodiac, nationality, region, personality, hobby, photos, created_at from users where id = $1', [req.user.id]);
    if (!rows.length) return res.status(404).json({ error: '用户不存在' });
    const user = rows[0];
    const avatar = user.avatar_url
      ? user.avatar_url.startsWith('http')
        ? user.avatar_url
        : `${req.protocol}://${req.get('host')}${user.avatar_url}`
      : null;
    res.json({ ...user, avatar_url: avatar });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取用户信息失败' });
  }
});

// 更新个人信息（部分字段）
app.patch('/api/me', authenticateJWT, async (req, res) => {
  const {
    name,
    age,
    gender,
    zodiac,
    nationality,
    region,
    personality,
    hobby,
    photos,
  } = req.body || {};
  try {
    const sets = [];
    const values = [];
    let idx = 1;
    const push = (field, val) => {
      sets.push(`${field} = $${idx}`);
      values.push(val);
      idx += 1;
    };
    if (name !== undefined) push('name', name);
    if (age !== undefined) push('age', age);
    if (gender !== undefined) push('gender', gender);
    if (zodiac !== undefined) push('zodiac', zodiac);
    if (nationality !== undefined) push('nationality', nationality);
    if (region !== undefined) push('region', region);
    if (personality !== undefined) push('personality', personality);
    if (hobby !== undefined) push('hobby', hobby);
    if (photos !== undefined) {
      const arr = Array.isArray(photos) ? photos.slice(0, 5) : null;
      push('photos', arr);
    }
    if (!sets.length) return res.status(400).json({ error: '无可更新字段' });
    values.push(req.user.id);
    const sql = `update users set ${sets.join(', ')} where id = $${idx} returning id, phone_code, phone, name, avatar_url, age, gender, zodiac, nationality, region, personality, hobby, photos, created_at`;
    const { rows } = await pool.query(sql, values);
    const user = rows[0];
    const avatar = user.avatar_url
      ? user.avatar_url.startsWith('http')
        ? user.avatar_url
        : `${req.protocol}://${req.get('host')}${user.avatar_url}`
      : null;
    res.json({ ...user, avatar_url: avatar });
  } catch (err) {
    console.error(err);
    if (err.code === '23505') {
      return res.status(409).json({ error: '昵称已被占用' });
    }
    res.status(500).json({ error: '更新失败' });
  }
});

// Admin stats
app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  try {
    const totalRes = await pool.query('select count(*)::int as count from users');
    const dailyRes = await pool.query("select count(*)::int as count from users where created_at >= date_trunc('day', now())");
    let dailyMatches = 0;
    try {
      const exists = await pool.query("select to_regclass('public.matches') as name");
      if (exists.rows[0]?.name) {
        const matchRes = await pool.query("select count(*)::int as count from matches where created_at >= date_trunc('day', now())");
        dailyMatches = matchRes.rows[0]?.count ?? 0;
      }
    } catch (_) {
      dailyMatches = 0;
    }
    res.json({
      daily_new_users: totalRes.rows[0] && dailyRes.rows[0] ? dailyRes.rows[0].count : 0,
      total_users: totalRes.rows[0]?.count ?? 0,
      daily_matches: dailyMatches,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取统计失败' });
  }
});

// Admin users list
app.get('/api/admin/users', requireAdmin, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 200);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const { rows } = await pool.query(
      `select id, phone_code, phone, name, avatar_url, age, gender, zodiac, personality, hobby, photos,
        coalesce(match_enabled, false) as match_enabled, created_at
       from users
       order by created_at desc
       limit $1 offset $2`,
      [limit, offset]
    );
    const users = rows.map((u) => ({
      ...u,
      avatar_url: toPublicUrl(req, u.avatar_url),
      photos: Array.isArray(u.photos) ? u.photos.map((p) => toPublicUrl(req, p)).filter(Boolean) : [],
    }));
    res.json({ users });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '获取用户失败' });
  }
});

// Admin toggle match
app.patch('/api/admin/users/:id/match', requireAdmin, async (req, res) => {
  const { match_enabled } = req.body || {};
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) return res.status(400).json({ error: '用户ID无效' });
  if (typeof match_enabled !== 'boolean') return res.status(400).json({ error: 'match_enabled 必须为布尔值' });
  try {
    if (match_enabled) {
      const photoRes = await pool.query('select photos from users where id = $1', [id]);
      const photos = Array.isArray(photoRes.rows[0]?.photos) ? photoRes.rows[0].photos : [];
      if (photos.length === 0) {
        return res.status(400).json({ error: '相册为空，无法开启匹配' });
      }
    }
    const { rows } = await pool.query(
      'update users set match_enabled = $1 where id = $2 returning id, match_enabled',
      [match_enabled, id]
    );
    if (!rows.length) return res.status(404).json({ error: '用户不存在' });
    res.json(rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '更新失败' });
  }
});

// 注册流程：预上传头像（无鉴权，仍限 2MB）
app.post('/api/register/avatar', upload.single('avatar'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: '未收到文件' });
    if (!supabase) return res.status(500).json({ error: '存储服务未配置' });

    const buf = req.file.buffer;
    const ext = sniffImageExt(buf, req.file.originalname);
    if (!ext) return res.status(400).json({ error: '文件格式不支持' });
    const bucket = process.env.SUPABASE_BUCKET || 'avatars';
    const objectKey = `register/temp_${Date.now()}${ext}`;

    const { error: uploadErr } = await supabase.storage
      .from(bucket)
      .upload(objectKey, buf, { contentType: req.file.mimetype, upsert: true });
    if (uploadErr) throw uploadErr;

    const { data: pub } = supabase.storage.from(bucket).getPublicUrl(objectKey);
    const publicUrl = pub?.publicUrl || null;
    res.json({ avatar_url: publicUrl || objectKey });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '上传头像失败' });
  }
});

// Avatar upload (auth required) -> Supabase Storage
app.post('/api/me/avatar', authenticateJWT, upload.single('avatar'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: '未收到文件' });
    if (!supabase) return res.status(500).json({ error: '存储服务未配置' });

    const buf = req.file.buffer;
    const ext = sniffImageExt(buf, req.file.originalname);
    if (!ext) return res.status(400).json({ error: '文件格式不支持' });
    const bucket = process.env.SUPABASE_BUCKET || 'avatars';
    const objectKey = `${req.user.id}/avatar_${Date.now()}${ext}`;

    // 删除旧头像（同 bucket）或本地遗留
    try {
      const { rows } = await pool.query('select avatar_url from users where id = $1', [req.user.id]);
      const old = rows[0]?.avatar_url;
      if (old && old.includes('/storage/v1/object/public/')) {
        const parts = old.split('/storage/v1/object/public/')[1];
        if (parts) {
          const oldKey = parts.replace(`${bucket}/`, '');
          await supabase.storage.from(bucket).remove([oldKey]);
        }
      } else if (old && old.startsWith('/uploads/avatars/')) {
        const oldPath = path.join(__dirname, old.replace('/uploads/', 'uploads/'));
        fs.promises.unlink(oldPath).catch(() => {});
      }
    } catch (_) {}

    const { error: uploadErr } = await supabase.storage
      .from(bucket)
      .upload(objectKey, buf, { contentType: req.file.mimetype, upsert: true });
    if (uploadErr) throw uploadErr;

    const { data: pub } = supabase.storage.from(bucket).getPublicUrl(objectKey);
    const publicUrl = pub?.publicUrl || null;

    await pool.query('update users set avatar_url = $1 where id = $2', [publicUrl || objectKey, req.user.id]);
    res.json({ avatar_url: publicUrl || objectKey });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '上传头像失败' });
  }
});

// Album photo upload (auth required) -> Supabase Storage
app.post('/api/me/photos', authenticateJWT, upload.single('photo'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: '未收到文件' });
    if (!supabase) return res.status(500).json({ error: '存储服务未配置' });

    const buf = req.file.buffer;
    const ext = sniffImageExt(buf, req.file.originalname);
    if (!ext) return res.status(400).json({ error: '文件格式不支持' });

    const { rows } = await pool.query('select photos from users where id = $1', [req.user.id]);
    const current = Array.isArray(rows[0]?.photos) ? rows[0].photos : [];
    if (current.length >= 5) return res.status(400).json({ error: '相册最多 5 张' });

    const bucket = process.env.SUPABASE_BUCKET || 'avatars';
    const suffix = Math.random().toString(36).slice(2, 8);
    const objectKey = `${req.user.id}/photos/photo_${Date.now()}_${suffix}${ext}`;

    const { error: uploadErr } = await supabase.storage
      .from(bucket)
      .upload(objectKey, buf, { contentType: req.file.mimetype, upsert: true });
    if (uploadErr) throw uploadErr;

    const { data: pub } = supabase.storage.from(bucket).getPublicUrl(objectKey);
    const publicUrl = pub?.publicUrl || objectKey;
    const next = [...current, publicUrl].slice(0, 5);

    await pool.query('update users set photos = $1 where id = $2', [next, req.user.id]);
    res.json({ photos: next, added: publicUrl });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: '上传照片失败' });
  }
});

const port = process.env.PORT || 4000;
const server = http.createServer(app);

io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST', 'PATCH'] },
});

io.use(async (socket, next) => {
  const authToken = socket.handshake.auth?.token;
  const headerAuth = socket.handshake.headers?.authorization || '';
  const bearer = headerAuth.startsWith('Bearer ') ? headerAuth.slice(7) : '';
  const token = authToken || bearer;
  const payload = verifyToken(token);
  if (payload?.id) {
    socket.data.userId = payload.id;
    return next();
  }
  const guestToken =
    socket.handshake.auth?.guest_token ||
    socket.handshake.headers?.['x-guest-token'];
  if (typeof guestToken === 'string' && guestToken.trim()) {
    try {
      const guestId = await ensureGuestUser(guestToken.trim());
      if (guestId) {
        socket.data.userId = guestId;
        return next();
      }
    } catch (err) {
      return next(new Error('unauthorized'));
    }
  }
  return next(new Error('unauthorized'));
});

io.on('connection', (socket) => {
  const userId = socket.data.userId;
  if (userId) {
    socket.join(`user:${userId}`);
    const count = touchOnline(userId);
    if (count === 1) emitPresence(userId, true);
  }

  socket.on('presence:watch', (payload = {}) => {
    const targetId = Number(payload.user_id);
    if (!Number.isFinite(targetId)) return;
    socket.join(`presence:${targetId}`);
    socket.emit('presence:update', {
      user_id: targetId,
      online: isUserOnline(targetId),
      last_seen: lastSeenMap.get(targetId) || null,
    });
  });

  socket.on('presence:unwatch', (payload = {}) => {
    const targetId = Number(payload.user_id);
    if (!Number.isFinite(targetId)) return;
    socket.leave(`presence:${targetId}`);
  });

  socket.on('disconnect', () => {
    if (!userId) return;
    const count = touchOffline(userId);
    if (count === 0) emitPresence(userId, false);
  });
});

async function setupRedisAdapter() {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) return;
  if (!createAdapter || !createClient) {
    console.warn('[socket] REDIS_URL 已设置，但未安装 @socket.io/redis-adapter 或 redis，已跳过');
    return;
  }
  const pubClient = createClient({ url: redisUrl });
  const subClient = pubClient.duplicate();
  pubClient.on('error', (err) => console.error('[redis]', err));
  subClient.on('error', (err) => console.error('[redis]', err));
  await Promise.all([pubClient.connect(), subClient.connect()]);
  io.adapter(createAdapter(pubClient, subClient));
  console.log('[socket] redis adapter enabled');
}

async function backfillChatLastMessage() {
  try {
    await pool.query(
      `update chats c
       set last_message_at = m.created_at,
           last_message = m.content,
           last_message_image = m.image_url
       from (
         select distinct on (chat_id) chat_id, content, image_url, created_at
         from messages
         order by chat_id, created_at desc
       ) m
       where c.id = m.chat_id
         and (c.last_message_at is null or c.last_message is null)`
    );
  } catch (err) {
    console.error('[chat] backfill last_message failed', err);
  }
}

async function cleanupGuests() {
  const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  try {
    await pool.query(
      `update users
       set guest_expired_at = now(), guest_token = null
       where is_guest = true
         and guest_expired_at is null
         and created_at < $1`,
      [cutoff]
    );

    await pool.query(
      `update chats c
       set guest_expired = true, guest_expired_at = now()
       from (
         select c.id
         from chats c
         join chat_participants p on p.chat_id = c.id
         join users u on u.id = p.user_id
         group by c.id
         having bool_or(u.is_guest = true and u.guest_expired_at is not null)
            and bool_or(u.is_guest = false)
       ) t
       where c.id = t.id
         and coalesce(c.guest_expired, false) = false`
    );
    try {
      const expiredRes = await pool.query(
        `select c.id
         from chats c
         join chat_participants p on p.chat_id = c.id
         join users u on u.id = p.user_id
         group by c.id
         having bool_or(u.is_guest = true and u.guest_expired_at is not null)
            and bool_or(u.is_guest = false)`
      );
      expiredRes.rows.forEach((r) => {
        setChatStatusCache(r.id, { guest_expired: true });
      });
    } catch (_) {}

    const guestChatsRes = await pool.query(
      `select c.id
       from chats c
       join chat_participants p on p.chat_id = c.id
       join users u on u.id = p.user_id
       group by c.id
       having bool_and(u.is_guest = true)
          and max(u.created_at) < $1`,
      [cutoff]
    );
    const guestChatIds = guestChatsRes.rows.map((r) => r.id);
    if (guestChatIds.length) {
      await pool.query('delete from messages where chat_id = any($1)', [guestChatIds]);
      await pool.query('delete from chat_participants where chat_id = any($1)', [guestChatIds]);
      await pool.query('delete from chats where id = any($1)', [guestChatIds]);
      guestChatIds.forEach((id) => {
        messagesCache.delete(String(id));
        chatParticipantsCache.delete(String(id));
        chatStatusCache.delete(String(id));
      });
    }

    await pool.query(
      `delete from users u
       where u.is_guest = true
         and u.created_at < $1
         and not exists (select 1 from chat_participants p where p.user_id = u.id)`,
      [cutoff]
    );
  } catch (err) {
    console.error('[guest] cleanup failed', err);
  }
}

async function start() {
  await ensureSchema();
  await backfillChatLastMessage();
  await cleanupGuests();
  setInterval(cleanupGuests, 60 * 60 * 1000);
  await setupRedisAdapter();
  server.listen(port, () => console.log(`API running on http://localhost:${port}`));
}

start().catch((err) => {
  console.error('启动失败', err);
  process.exit(1);
});
