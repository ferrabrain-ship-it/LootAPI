import { createClient } from '@supabase/supabase-js'
import type { Address } from 'viem'
import { env } from '../config/env.js'

const supabase = env.supabaseUrl && env.supabaseServiceRoleKey
  ? createClient(env.supabaseUrl, env.supabaseServiceRoleKey, { auth: { persistSession: false } })
  : null

export interface ProfileShape {
  address: string
  username: string | null
  bio: string | null
  pfpUrl: string | null
  bannerUrl?: string | null
  discord: string | null
}

export async function getProfile(address: Address): Promise<ProfileShape> {
  if (!supabase) {
    return {
      address,
      username: null,
      bio: null,
      pfpUrl: null,
      bannerUrl: null,
      discord: null,
    }
  }

  const { data } = await supabase
    .from('profiles')
    .select('wallet_address,username,bio,pfp_url,banner_url,discord')
    .eq('wallet_address', address.toLowerCase())
    .maybeSingle()

  return {
    address,
    username: data?.username ?? null,
    bio: data?.bio ?? null,
    pfpUrl: data?.pfp_url ?? null,
    bannerUrl: data?.banner_url ?? null,
    discord: data?.discord ?? null,
  }
}

export async function getProfilesBatch(addresses: Address[]) {
  if (!addresses.length) return { profiles: [] }

  if (!supabase) {
    return {
      profiles: addresses.map((address) => ({
        address,
        username: null,
        pfpUrl: null,
      })),
    }
  }

  const lower = addresses.map((a) => a.toLowerCase())
  const { data } = await supabase
    .from('profiles')
    .select('wallet_address,username,pfp_url')
    .in('wallet_address', lower)

  const byAddress = new Map((data ?? []).map((row) => [
    row.wallet_address.toLowerCase(),
    { address: row.wallet_address, username: row.username ?? null, pfpUrl: row.pfp_url ?? null },
  ]))

  return {
    profiles: addresses.map((address) => byAddress.get(address.toLowerCase()) ?? {
      address,
      username: null,
      pfpUrl: null,
    }),
  }
}
