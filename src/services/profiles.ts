import type { Address } from 'viem'
import { supabaseAdmin as supabase } from '../lib/supabase.js'

export interface ProfileShape {
  address: string
  username: string | null
  bio: string | null
  pfpUrl: string | null
  bannerUrl?: string | null
  discord: string | null
}

interface ProfileRow {
  wallet_address: string
  username: string | null
  bio?: string | null
  pfp_url: string | null
  banner_url?: string | null
  discord?: string | null
}

interface SocialConnectionRow {
  wallet_address: string
  twitter_handle: string | null
  discord_username: string | null
}

const PROFILE_CACHE_TTL_MS = 30_000
const PROFILE_QUERY_TIMEOUT_MS = 3_000
const profileCache = new Map<string, { expiresAt: number; value: ProfileShape }>()
const batchCache = new Map<string, { expiresAt: number; value: { profiles: Array<{ address: string; username: string | null; pfpUrl: string | null }> } }>()

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, fallback: T): Promise<T> {
  let timer: NodeJS.Timeout | null = null

  return Promise.race([
    promise.finally(() => {
      if (timer) clearTimeout(timer)
    }),
    new Promise<T>((resolve) => {
      timer = setTimeout(() => resolve(fallback), timeoutMs)
    }),
  ])
}

function getCachedProfile(key: string) {
  const cached = profileCache.get(key)
  if (!cached) return null
  if (cached.expiresAt <= Date.now()) {
    profileCache.delete(key)
    return null
  }
  return cached.value
}

function setCachedProfile(key: string, value: ProfileShape) {
  profileCache.set(key, { expiresAt: Date.now() + PROFILE_CACHE_TTL_MS, value })
  while (profileCache.size > 1000) {
    const oldestKey = profileCache.keys().next().value
    if (oldestKey === undefined) break
    profileCache.delete(oldestKey)
  }
}

function getCachedBatch(key: string) {
  const cached = batchCache.get(key)
  if (!cached) return null
  if (cached.expiresAt <= Date.now()) {
    batchCache.delete(key)
    return null
  }
  return cached.value
}

function setCachedBatch(
  key: string,
  value: { profiles: Array<{ address: string; username: string | null; pfpUrl: string | null }> }
) {
  batchCache.set(key, { expiresAt: Date.now() + PROFILE_CACHE_TTL_MS, value })
  while (batchCache.size > 256) {
    const oldestKey = batchCache.keys().next().value
    if (oldestKey === undefined) break
    batchCache.delete(oldestKey)
  }
}

function resolveDisplayName(profile?: Pick<ProfileRow, 'username'> | null, social?: Pick<SocialConnectionRow, 'twitter_handle' | 'discord_username'> | null) {
  return profile?.username ?? social?.twitter_handle ?? social?.discord_username ?? null
}

export async function getProfile(address: Address): Promise<ProfileShape> {
  const cached = getCachedProfile(address.toLowerCase())
  if (cached) return cached

  if (!supabase) {
    const emptyProfile = {
      address,
      username: null,
      bio: null,
      pfpUrl: null,
      bannerUrl: null,
      discord: null,
    }
    setCachedProfile(address.toLowerCase(), emptyProfile)
    return emptyProfile
  }

  const lowerAddress = address.toLowerCase()
  const [{ data: profile }, { data: social }] = await Promise.all([
    supabase
      .from('profiles')
      .select('wallet_address,username,bio,pfp_url,banner_url,discord')
      .eq('wallet_address', lowerAddress)
      .maybeSingle<ProfileRow>(),
    supabase
      .from('social_connections')
      .select('wallet_address,twitter_handle,discord_username')
      .eq('wallet_address', lowerAddress)
      .maybeSingle<SocialConnectionRow>(),
  ])

  const profileShape = {
    address,
    username: resolveDisplayName(profile, social),
    bio: profile?.bio ?? null,
    pfpUrl: profile?.pfp_url ?? null,
    bannerUrl: profile?.banner_url ?? null,
    discord: profile?.discord ?? social?.discord_username ?? null,
  }
  setCachedProfile(lowerAddress, profileShape)
  return profileShape
}

export async function getProfilesBatch(addresses: Address[]) {
  if (!addresses.length) return { profiles: [] }

  const batchKey = addresses.map((address) => address.toLowerCase()).sort().join(',')
  const cached = getCachedBatch(batchKey)
  if (cached) return cached

  if (!supabase) {
    const emptyBatch = {
      profiles: addresses.map((address) => ({
        address,
        username: null,
        pfpUrl: null,
      })),
    }
    setCachedBatch(batchKey, emptyBatch)
    return emptyBatch
  }

  const lower = addresses.map((a) => a.toLowerCase())
  const [{ data: profilesData }, { data: socialsData }] = await Promise.all([
    supabase
      .from('profiles')
      .select('wallet_address,username,pfp_url')
      .in('wallet_address', lower)
      .returns<ProfileRow[]>(),
    supabase
      .from('social_connections')
      .select('wallet_address,twitter_handle,discord_username')
      .in('wallet_address', lower)
      .returns<SocialConnectionRow[]>(),
  ])

  const profilesByAddress = new Map((profilesData ?? []).map((row) => [
    row.wallet_address.toLowerCase(),
    { address: row.wallet_address, username: row.username ?? null, pfpUrl: row.pfp_url ?? null },
  ]))
  const socialsByAddress = new Map((socialsData ?? []).map((row) => [
    row.wallet_address.toLowerCase(),
    row,
  ]))

  const result = {
    profiles: addresses.map((address) => {
      const lowerAddress = address.toLowerCase()
      const profile = profilesByAddress.get(lowerAddress)
      const social = socialsByAddress.get(lowerAddress)

      return {
        address,
        username: profile?.username ?? social?.twitter_handle ?? social?.discord_username ?? null,
        pfpUrl: profile?.pfpUrl ?? null,
      }
    }),
  }
  setCachedBatch(batchKey, result)
  return result
}
