import type { Address } from 'viem'

export const CONTRACTS = {
  loot: '0x0b6b68D329D0ebFD7C383835e93aDCea670e93f4' as Address,
  treasury: '0x1ac77Ea2b1467FFB9d830B07Bc56a299b36b9404' as Address,
  treasuryAgent: '0x8090188Eeae4B84Fd3cFB8f53AdccB30a0a17b21' as Address,
  gridMining: '0x5C37c4cF90094cbF8d7e44A82141BccbE9Aff93e' as Address,
  autoMiner: '0x9AccD84987d671e8092A000ACB225FE8dFB9096C' as Address,
  crown: '0xa4A2E37d9e98F8036ed3fa2653252D7bB7b50fEa' as Address,
  autoCrown: '0x49B28C128eB8aec3c6b5e14D4d850106E50C98Ba' as Address,
  lootCases: '0x5a516A7468dd2bb632a717EDd7B8413e16C1221a' as Address,
  goldCases: '0x5a516A7468dd2bb632a717EDd7B8413e16C1221a' as Address,
  staking: '0xe320f497E2029a4A20F8E0229D13B22e95D8d143' as Address,
  lootLocker: '0x2e975939B3aAEa243800774e5A8ffdB58292633a' as Address,
  lockerRewards: '0x6D130990399e649756948A8D451462fbd5A09709' as Address,
} as const

export const LEGACY_CONTRACTS = {
  loot: '0x00E701Eff4f9Dc647f1510f835C5d1ee7E41D28f' as Address,
  treasury: '0x89885D1E97e211B6DeC8436F7E3456b06EB24c68' as Address,
  gridMining: '0xA8E2F506aDcbBF18733A9F0f32e3D70b1A34d723' as Address,
  autoMiner: '0x4b99Ebe4F9220Bd5206199b10dFC039a6a73eDBC' as Address,
  crown: '0x25f3064b32feAa099108b1d7a7a6C3F665536bDe' as Address,
  autoCrown: '0x8658c4e7b193b7FBD1a342996f5B4e6F879D1816' as Address,
  goldCases: '0x79B12f973b6BACc0A0420ee5643B61c7d8C70631' as Address,
  staking: '0x554CEAe7b091b21DdAeFe65cF79651132Ee84Ed7' as Address,
  lootLocker: '0xbb9D524e28c7E7b5A9D439D5D1ba68A87788BbB6' as Address,
  lockerRewards: '0x066F53c33Bcba938625dfa3741cb92C1b0C7064a' as Address,
} as const

export const ACTIVE_STACK_KEY = 'lootgames-v1'

export const PROTOCOL_CONSTANTS = {
  gridSize: 25,
  adminFeeBps: 100n,
  vaultFeeBps: 1000n,
  bpsDenominator: 10000n,
  initialSupply: 10_000n * 10n ** 18n,
  maxSupply: 10_000n * 10n ** 18n,
}
