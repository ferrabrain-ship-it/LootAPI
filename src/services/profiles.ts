import type { Address } from 'viem'
import { env } from '../config/env.js'
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

function resolveDisplayName(profile?: Pick<ProfileRow, 'username'> | null, social?: Pick<SocialConnectionRow, 'twitter_handle' | 'discord_username'> | null) {
  return profile?.username ?? social?.twitter_handle ?? social?.discord_username ?? null
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

  return {
    address,
    username: resolveDisplayName(profile, social),
    bio: profile?.bio ?? null,
    pfpUrl: profile?.pfp_url ?? null,
    bannerUrl: profile?.banner_url ?? null,
    discord: profile?.discord ?? social?.discord_username ?? null,
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

  return {
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
}
