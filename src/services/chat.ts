import type { Address } from 'viem'
import { verifyMessage } from 'viem'
import { supabaseAdmin as supabase } from '../lib/supabase.js'

const MAX_MESSAGE_LENGTH = 280
const MAX_LIMIT = 80
const SESSION_TTL_SECONDS = 12 * 60 * 60
const RATE_LIMIT_MS = 1200
const MAX_REACTION_LENGTH = 16
const MEMORY_MESSAGE_LIMIT = 200
const CHAT_FULL_SELECT = 'id,wallet_address,text,created_at,reply_to_id,reactions'
const CHAT_BASE_SELECT = 'id,wallet_address,text,created_at'

export type ChatReaction = {
  emoji: string
  count: number
  reacted: boolean
}

export type ChatReply = {
  id: string
  address: string
  text: string
  createdAt: string
}

export type ChatMessage = {
  id: string
  address: string
  text: string
  createdAt: string
  replyToId: string | null
  reply: ChatReply | null
  reactions: ChatReaction[]
  reactionUsers?: StoredReactionMap
}

type StoredChatRow = {
  id?: string | number | null
  wallet_address?: string | null
  text?: string | null
  created_at?: string | null
  reply_to_id?: string | number | null
  reactions?: unknown
}

type StoredReactionMap = Record<string, string[]>

type ChatSession = {
  signature?: unknown
  issuedAt?: unknown
}

type ChatMemory = typeof globalThis & {
  __minelootChatMessages?: ChatMessage[]
  __minelootChatRateLimit?: Map<string, number>
}

const memory = globalThis as ChatMemory

export class ChatError extends Error {
  constructor(message: string, public readonly status: number) {
    super(message)
  }
}

function getMemoryMessages() {
  if (!memory.__minelootChatMessages) memory.__minelootChatMessages = []
  return memory.__minelootChatMessages
}

function getRateLimit() {
  if (!memory.__minelootChatRateLimit) memory.__minelootChatRateLimit = new Map()
  return memory.__minelootChatRateLimit
}

function normalizeAddress(address: unknown): Address | null {
  if (typeof address !== 'string') return null
  const normalized = address.toLowerCase()
  return /^0x[a-f0-9]{40}$/.test(normalized) ? normalized as Address : null
}

function normalizeText(text: unknown) {
  if (typeof text !== 'string') return null
  const trimmed = text.replace(/\s+/g, ' ').trim()
  if (!trimmed || trimmed.length > MAX_MESSAGE_LENGTH) return null
  return trimmed
}

function normalizeMessageId(messageId: unknown) {
  if (typeof messageId !== 'string') return null
  const trimmed = messageId.trim()
  if (!trimmed || trimmed.length > 120) return null
  return trimmed
}

function normalizeEmoji(emoji: unknown) {
  if (typeof emoji !== 'string') return null
  const trimmed = emoji.trim()
  if (!trimmed || trimmed.length > MAX_REACTION_LENGTH) return null
  return trimmed
}

function sessionMessage(address: string, issuedAt: number) {
  return `MineLoot Chat Session\nAddress: ${address}\nIssued At: ${issuedAt}`
}

function normalizeReactionMap(value: unknown): StoredReactionMap {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}

  const next: StoredReactionMap = {}
  for (const [emoji, users] of Object.entries(value)) {
    const normalizedEmoji = normalizeEmoji(emoji)
    if (!normalizedEmoji || !Array.isArray(users)) continue
    const normalizedUsers = Array.from(new Set(users.map(normalizeAddress).filter(Boolean) as string[]))
    if (normalizedUsers.length > 0) {
      next[normalizedEmoji] = normalizedUsers
    }
  }
  return next
}

function summarizeReactions(value: unknown, viewerAddress?: string | null): ChatReaction[] {
  const reactions = normalizeReactionMap(value)
  return Object.entries(reactions)
    .map(([emoji, users]) => ({
      emoji,
      count: users.length,
      reacted: viewerAddress ? users.includes(viewerAddress) : false,
    }))
    .filter((reaction) => reaction.count > 0)
}

function withoutReactionUsers(message: ChatMessage): ChatMessage {
  return {
    id: message.id,
    address: message.address,
    text: message.text,
    createdAt: message.createdAt,
    replyToId: message.replyToId,
    reply: message.reply,
    reactions: message.reactions,
  }
}

function mapRow(row: StoredChatRow, viewerAddress?: string | null): ChatMessage | null {
  const address = normalizeAddress(row.wallet_address)
  const text = normalizeText(row.text)
  if (!address || !text) return null

  return {
    id: String(row.id ?? `${address}-${row.created_at ?? Date.now()}`),
    address,
    text,
    createdAt: row.created_at ?? new Date().toISOString(),
    replyToId: row.reply_to_id == null ? null : String(row.reply_to_id),
    reply: null,
    reactions: summarizeReactions(row.reactions, viewerAddress),
  }
}

function attachReplies(messages: ChatMessage[]) {
  const byId = new Map(messages.map((message) => [message.id, message]))
  return messages.map((message) => {
    if (!message.replyToId) return message
    const reply = byId.get(message.replyToId)
    if (!reply) return message
    return {
      ...message,
      reply: {
        id: reply.id,
        address: reply.address,
        text: reply.text,
        createdAt: reply.createdAt,
      },
    }
  })
}

function logChatStorageFallback(operation: string, error: unknown) {
  const detail = error && typeof error === 'object' && 'message' in error
    ? String((error as { message?: unknown }).message)
    : String(error ?? 'unknown')
  console.warn(`[chat] falling back to in-memory ${operation}:`, detail)
}

function mapSupabaseRows(rows: unknown[] | null, viewerAddress?: string | null) {
  const mapped = (rows ?? [])
    .map((row) => mapRow(row as StoredChatRow, viewerAddress))
    .filter((message): message is ChatMessage => Boolean(message))
    .reverse()

  return attachReplies(mapped)
}

async function readSupabaseMessages(limit: number, viewerAddress?: string | null) {
  if (!supabase) return null

  const { data, error } = await supabase
    .from('chat_messages')
    .select(CHAT_FULL_SELECT)
    .order('created_at', { ascending: false })
    .limit(limit)

  if (!error) return mapSupabaseRows(data, viewerAddress)

  const fallback = await supabase
    .from('chat_messages')
    .select(CHAT_BASE_SELECT)
    .order('created_at', { ascending: false })
    .limit(limit)

  if (fallback.error) {
    logChatStorageFallback('messages', fallback.error)
    return null
  }

  return mapSupabaseRows(fallback.data, viewerAddress)
}

async function insertSupabaseMessage(address: string, text: string, replyToId: string | null, viewerAddress?: string | null) {
  if (!supabase) return null

  const { data, error } = await supabase
    .from('chat_messages')
    .insert({ wallet_address: address, text, reply_to_id: replyToId })
    .select(CHAT_FULL_SELECT)
    .single()

  if (!error) return mapRow(data as StoredChatRow, viewerAddress)

  const fallback = await supabase
    .from('chat_messages')
    .insert({ wallet_address: address, text })
    .select(CHAT_BASE_SELECT)
    .single()

  if (fallback.error) {
    logChatStorageFallback('insert', fallback.error)
    return null
  }

  return mapRow(fallback.data as StoredChatRow, viewerAddress)
}

function readMemoryMessages(limit: number, viewerAddress?: string | null) {
  return attachReplies(
    getMemoryMessages()
      .slice(-limit)
      .map((message) => {
        const { reactionUsers, ...publicMessage } = message
        return {
          ...publicMessage,
          reactions: summarizeReactions(reactionUsers ?? {}, viewerAddress),
        }
      })
  )
}

function insertMemoryMessage(address: string, text: string, replyToId: string | null) {
  const message: ChatMessage = {
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    address,
    text,
    createdAt: new Date().toISOString(),
    replyToId,
    reply: null,
    reactions: [],
    reactionUsers: {},
  }
  getMemoryMessages().push(message)
  if (getMemoryMessages().length > MEMORY_MESSAGE_LIMIT) {
    memory.__minelootChatMessages = getMemoryMessages().slice(-MEMORY_MESSAGE_LIMIT)
  }
  return withoutReactionUsers(message)
}

async function verifyChatSession(address: string, session: unknown) {
  if (!session || typeof session !== 'object') {
    throw new ChatError('Missing chat signature', 401)
  }

  const rawSignature = (session as ChatSession).signature
  const rawIssuedAt = (session as ChatSession).issuedAt
  const signature = typeof rawSignature === 'string' ? rawSignature : null
  const issuedAt = typeof rawIssuedAt === 'number' ? rawIssuedAt : null
  if (!signature || !issuedAt) throw new ChatError('Missing chat signature', 401)

  const nowSeconds = Math.floor(Date.now() / 1000)
  if (Math.abs(nowSeconds - issuedAt) > SESSION_TTL_SECONDS) {
    throw new ChatError('Chat signature expired', 401)
  }

  try {
    const valid = await verifyMessage({
      address: address as Address,
      message: sessionMessage(address, issuedAt),
      signature: signature as `0x${string}`,
    })
    if (!valid) throw new ChatError('Invalid chat signature', 401)
  } catch (error) {
    if (error instanceof ChatError) throw error
    throw new ChatError('Invalid chat signature', 401)
  }
}

async function toggleSupabaseReaction(messageId: string, address: string, emoji: string) {
  if (!supabase) return null

  const { data: existing, error: readError } = await supabase
    .from('chat_messages')
    .select('id,reactions')
    .eq('id', messageId)
    .single()

  if (readError || !existing) return null

  const reactions = normalizeReactionMap((existing as StoredChatRow).reactions)
  const users = new Set(reactions[emoji] ?? [])
  if (users.has(address)) {
    users.delete(address)
  } else {
    users.add(address)
  }

  if (users.size === 0) {
    delete reactions[emoji]
  } else {
    reactions[emoji] = Array.from(users)
  }

  const { data, error } = await supabase
    .from('chat_messages')
    .update({ reactions })
    .eq('id', messageId)
    .select(CHAT_FULL_SELECT)
    .single()

  if (error) return null
  return mapRow(data as StoredChatRow, address)
}

function toggleMemoryReaction(messageId: string, address: string, emoji: string) {
  const messages = getMemoryMessages()
  const index = messages.findIndex((message) => message.id === messageId)
  if (index < 0) return null

  const current = messages[index]
  const reactions = normalizeReactionMap(current.reactionUsers ?? {})
  const users = new Set(reactions[emoji] ?? [])
  if (users.has(address)) {
    users.delete(address)
  } else {
    users.add(address)
  }

  if (users.size === 0) {
    delete reactions[emoji]
  } else {
    reactions[emoji] = Array.from(users)
  }

  const updated = {
    ...current,
    reactions: summarizeReactions(reactions, address),
    reactionUsers: reactions,
  }
  messages[index] = updated
  return withoutReactionUsers(updated)
}

export async function getChatMessages(rawLimit: unknown, rawViewer?: unknown) {
  const parsedLimit = Number(rawLimit ?? 50)
  const limit = Number.isFinite(parsedLimit)
    ? Math.min(Math.max(Math.floor(parsedLimit), 1), MAX_LIMIT)
    : 50
  const viewer = normalizeAddress(rawViewer)
  const messages = await readSupabaseMessages(limit, viewer)

  return {
    messages: messages ?? readMemoryMessages(limit, viewer),
    storage: messages ? 'supabase' : 'memory',
  }
}

export async function createChatMessage(body: unknown) {
  if (!body || typeof body !== 'object') {
    throw new ChatError('Invalid body', 400)
  }

  const address = normalizeAddress((body as { walletAddress?: unknown }).walletAddress)
  const text = normalizeText((body as { text?: unknown }).text)
  const replyToId = normalizeMessageId((body as { replyToId?: unknown }).replyToId)
  const session = (body as { session?: unknown }).session

  if (!address) throw new ChatError('Invalid wallet address', 400)
  if (!text) throw new ChatError(`Message must be 1-${MAX_MESSAGE_LENGTH} characters`, 400)
  await verifyChatSession(address, session)

  const rateLimit = getRateLimit()
  const lastSent = rateLimit.get(address) ?? 0
  if (Date.now() - lastSent < RATE_LIMIT_MS) {
    throw new ChatError('Slow down', 429)
  }
  rateLimit.set(address, Date.now())

  const stored = await insertSupabaseMessage(address, text, replyToId, address)
  return { message: stored ?? insertMemoryMessage(address, text, replyToId) }
}

export async function toggleChatReaction(body: unknown) {
  if (!body || typeof body !== 'object') {
    throw new ChatError('Invalid body', 400)
  }

  const address = normalizeAddress((body as { walletAddress?: unknown }).walletAddress)
  const messageId = normalizeMessageId((body as { messageId?: unknown }).messageId)
  const emoji = normalizeEmoji((body as { emoji?: unknown }).emoji)
  const session = (body as { session?: unknown }).session

  if (!address) throw new ChatError('Invalid wallet address', 400)
  if (!messageId) throw new ChatError('Invalid message id', 400)
  if (!emoji) throw new ChatError('Invalid reaction', 400)
  await verifyChatSession(address, session)

  const stored = await toggleSupabaseReaction(messageId, address, emoji)
  const message = stored ?? toggleMemoryReaction(messageId, address, emoji)
  if (!message) throw new ChatError('Message not found', 404)

  return { message }
}
