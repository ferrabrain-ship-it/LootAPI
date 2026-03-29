import type { Address } from 'viem'

export const CONTRACTS = {
  loot: '0x00E701Eff4f9Dc647f1510f835C5d1ee7E41D28f' as Address,
  treasury: '0x89885D1E97e211B6DeC8436F7E3456b06EB24c68' as Address,
  treasuryAgent: '0x8090188Eeae4B84Fd3cFB8f53AdccB30a0a17b21' as Address,
  gridMining: '0xA8E2F506aDcbBF18733A9F0f32e3D70b1A34d723' as Address,
  autoMiner: '0x4b99Ebe4F9220Bd5206199b10dFC039a6a73eDBC' as Address,
  staking: '0x554CEAe7b091b21DdAeFe65cF79651132Ee84Ed7' as Address,
  lootLocker: '0xbb9D524e28c7E7b5A9D439D5D1ba68A87788BbB6' as Address,
  lockerRewards: '0x066F53c33Bcba938625dfa3741cb92C1b0C7064a' as Address,
} as const

export const PROTOCOL_CONSTANTS = {
  gridSize: 25,
  adminFeeBps: 100n,
  vaultFeeBps: 1000n,
  bpsDenominator: 10000n,
  maxSupply: 3_000_000n * 10n ** 18n,
}
