-- MineLoot Dune Dashboard Starter (Base)
-- -------------------------------------------------------------------
-- 1) Replace `mineloot_base` with your decoded schema name in Dune.
-- 2) Run one query block at a time in Dune.
-- 3) All token/ETH amounts are normalized to human units (1e18).
--
-- Contracts (for reference):
-- LOOT          0x00E701Eff4f9Dc647f1510f835C5d1ee7E41D28f
-- GridMining    0xA8E2F506aDcbBF18733A9F0f32e3D70b1A34d723
-- Treasury      0x89885D1E97e211B6DeC8436F7E3456b06EB24c68
-- Staking       0x554CEAe7b091b21DdAeFe65cF79651132Ee84Ed7
-- LootLocker    0xbb9D524e28c7E7b5A9D439D5D1ba68A87788BbB6
-- LockerRewards 0x066F53c33Bcba938625dfa3741cb92C1b0C7064a

-- ===================================================================
-- Q0) Discover decoded schema/tables
-- ===================================================================
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema ILIKE '%mineloot%'
   OR table_name ILIKE '%GridMining_evt_%'
   OR table_name ILIKE '%Treasury_evt_%'
   OR table_name ILIKE '%Loot_evt_Transfer%'
ORDER BY 1, 2;

-- ===================================================================
-- Q1) Main KPI cards
-- max supply, minted, burned, circulating, protocol revenue, locked
-- ===================================================================
WITH
mints AS (
  SELECT COALESCE(SUM(value), 0) AS minted_wei
  FROM mineloot_base.Loot_evt_Transfer
  WHERE "from" = 0x0000000000000000000000000000000000000000
    AND "to" <> 0x0000000000000000000000000000000000000000
),
burns AS (
  SELECT COALESCE(SUM(value), 0) AS burned_wei
  FROM mineloot_base.Loot_evt_Transfer
  WHERE "to" = 0x0000000000000000000000000000000000000000
),
vault AS (
  SELECT COALESCE(SUM(amount), 0) AS vaulted_wei
  FROM mineloot_base.Treasury_evt_VaultReceived
),
locks AS (
  SELECT COALESCE(SUM(delta_wei), 0) AS locked_wei
  FROM (
    SELECT CAST(amount AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_Locked
    UNION ALL
    SELECT CAST(amountAdded AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_AddedToLock
    UNION ALL
    SELECT -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_Unlocked
  ) t
)
SELECT
  3000000 AS max_supply_loot,
  CAST(m.minted_wei AS DOUBLE) / 1e18 AS minted_loot,
  CAST(b.burned_wei AS DOUBLE) / 1e18 AS burned_loot,
  CAST((m.minted_wei - b.burned_wei) AS DOUBLE) / 1e18 AS circulating_loot,
  CAST(v.vaulted_wei AS DOUBLE) / 1e18 AS protocol_revenue_eth,
  CAST(l.locked_wei AS DOUBLE) / 1e18 AS locked_loot
FROM mints m
CROSS JOIN burns b
CROSS JOIN vault v
CROSS JOIN locks l;

-- ===================================================================
-- Q2) Round history (core mining table)
-- ===================================================================
SELECT
  evt_block_time,
  roundId AS round_id,
  winningBlock + 1 AS winning_block,
  CAST(totalWinnings AS DOUBLE) / 1e18 AS total_winnings_eth,
  CAST(topMinerReward AS DOUBLE) / 1e18 AS top_miner_reward_eth,
  CAST(lootpotAmount AS DOUBLE) / 1e18 AS lootpot_loot,
  isSplit AS is_split,
  CAST(winnersDeployed AS BIGINT) AS winners_deployed,
  evt_tx_hash
FROM mineloot_base.GridMining_evt_RoundSettled
ORDER BY roundId DESC
LIMIT 1000;

-- ===================================================================
-- Q3) Heatmap base (last 200 winning rounds)
-- Use this for block % distribution (cold -> hot)
-- ===================================================================
WITH settled AS (
  SELECT roundId, winningBlock + 1 AS block_id
  FROM mineloot_base.GridMining_evt_RoundSettled
  ORDER BY roundId DESC
  LIMIT 200
),
agg AS (
  SELECT
    block_id,
    COUNT(*) AS wins
  FROM settled
  GROUP BY 1
),
grid AS (
  SELECT x AS block_id
  FROM UNNEST(sequence(1, 25)) AS t(x)
)
SELECT
  g.block_id,
  COALESCE(a.wins, 0) AS wins_last_200,
  COALESCE(ROUND(100.0 * a.wins / 200.0, 2), 0.00) AS win_pct_last_200
FROM grid g
LEFT JOIN agg a ON a.block_id = g.block_id
ORDER BY g.block_id;

-- ===================================================================
-- Q4) Leaderboard miners (lifetime deployed + rounds played)
-- ===================================================================
WITH deploys AS (
  SELECT user, roundId, totalAmount
  FROM mineloot_base.GridMining_evt_Deployed
  UNION ALL
  SELECT user, roundId, totalAmount
  FROM mineloot_base.GridMining_evt_DeployedFor
),
agg AS (
  SELECT
    user,
    COUNT(DISTINCT roundId) AS rounds_played,
    COALESCE(SUM(totalAmount), 0) AS deployed_wei
  FROM deploys
  GROUP BY 1
)
SELECT
  CONCAT('0x', LOWER(TO_HEX(user))) AS wallet,
  rounds_played,
  CAST(deployed_wei AS DOUBLE) / 1e18 AS total_deployed_eth
FROM agg
ORDER BY deployed_wei DESC
LIMIT 200;

-- ===================================================================
-- Q5) Leaderboard stakers (current net staked)
-- ===================================================================
WITH deltas AS (
  SELECT user, CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.Staking_evt_Deposited
  UNION ALL
  SELECT user, -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.Staking_evt_Withdrawn
),
agg AS (
  SELECT
    user,
    COALESCE(SUM(delta_wei), 0) AS staked_wei
  FROM deltas
  GROUP BY 1
)
SELECT
  CONCAT('0x', LOWER(TO_HEX(user))) AS wallet,
  CAST(staked_wei AS DOUBLE) / 1e18 AS staked_loot
FROM agg
WHERE staked_wei > 0
ORDER BY staked_wei DESC
LIMIT 200;

-- ===================================================================
-- Q6) Leaderboard lockers (current net locked)
-- ===================================================================
WITH deltas AS (
  SELECT user, CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_Locked
  UNION ALL
  SELECT user, CAST(amountAdded AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_AddedToLock
  UNION ALL
  SELECT user, -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_Unlocked
),
agg AS (
  SELECT
    user,
    COALESCE(SUM(delta_wei), 0) AS locked_wei
  FROM deltas
  GROUP BY 1
)
SELECT
  CONCAT('0x', LOWER(TO_HEX(user))) AS wallet,
  CAST(locked_wei AS DOUBLE) / 1e18 AS locked_loot
FROM agg
WHERE locked_wei > 0
ORDER BY locked_wei DESC
LIMIT 200;

-- ===================================================================
-- Q7) Revenue table (buybacks + direct burns)
-- Mirrors MineLoot API logic: direct burns included even without ETH spent.
-- ===================================================================
WITH buybacks AS (
  SELECT
    evt_block_time,
    evt_tx_hash,
    CAST(ethSpent AS DOUBLE) / 1e18 AS spent_eth,
    CAST(lootBurned AS DOUBLE) / 1e18 AS burned_loot,
    CAST(lootToStakers AS DOUBLE) / 1e18 AS yield_generated_loot
  FROM mineloot_base.Treasury_evt_BuybackExecuted
),
direct_burns AS (
  SELECT
    t.evt_block_time,
    t.evt_tx_hash,
    CAST(NULL AS DOUBLE) AS spent_eth,
    CAST(t.value AS DOUBLE) / 1e18 AS burned_loot,
    CAST(NULL AS DOUBLE) AS yield_generated_loot,
    CONCAT('0x', LOWER(TO_HEX(t."from"))) AS burned_by
  FROM mineloot_base.Loot_evt_Transfer t
  LEFT JOIN mineloot_base.Treasury_evt_BuybackExecuted b
    ON b.evt_tx_hash = t.evt_tx_hash
  WHERE t."to" = 0x0000000000000000000000000000000000000000
    AND b.evt_tx_hash IS NULL
)
SELECT
  evt_block_time,
  evt_tx_hash,
  'BUYBACK' AS row_type,
  spent_eth,
  burned_loot,
  yield_generated_loot,
  CAST(NULL AS VARCHAR) AS burned_by
FROM buybacks
UNION ALL
SELECT
  evt_block_time,
  evt_tx_hash,
  'BURN' AS row_type,
  spent_eth,
  burned_loot,
  yield_generated_loot,
  burned_by
FROM direct_burns
ORDER BY evt_block_time DESC
LIMIT 1000;

-- ===================================================================
-- Q8) Lock revenue table (RewardNotified stream)
-- ===================================================================
WITH lock_snapshot AS (
  WITH deltas AS (
    SELECT user, CAST(amount AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_Locked
    UNION ALL
    SELECT user, CAST(amountAdded AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_AddedToLock
    UNION ALL
    SELECT user, -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
    FROM mineloot_base.LootLocker_evt_Unlocked
  ),
  by_user AS (
    SELECT user, COALESCE(SUM(delta_wei), 0) AS locked_wei
    FROM deltas
    GROUP BY 1
  )
  SELECT
    COUNT_IF(locked_wei > 0) AS lockers,
    CAST(COALESCE(SUM(CASE WHEN locked_wei > 0 THEN locked_wei ELSE 0 END), 0) AS DOUBLE) / 1e18 AS locked_supply_loot
  FROM by_user
)
SELECT
  n.evt_block_time,
  n.evt_tx_hash,
  CAST(n.amount AS DOUBLE) / 1e18 AS spent_eth,
  CAST(n.distributedAmount AS DOUBLE) / 1e18 AS distributed_eth,
  CAST(n.unallocatedAmount AS DOUBLE) / 1e18 AS unallocated_eth,
  s.lockers,
  s.locked_supply_loot
FROM mineloot_base.LockerRewards_evt_RewardNotified n
CROSS JOIN lock_snapshot s
ORDER BY n.evt_block_time DESC
LIMIT 1000;

-- ===================================================================
-- Q9) Flywheel daily panel
-- revenue ETH -> buybacks -> burn -> staker yield -> lock rewards
-- ===================================================================
WITH revenue AS (
  SELECT DATE_TRUNC('day', evt_block_time) AS day, SUM(CAST(amount AS DOUBLE) / 1e18) AS revenue_eth
  FROM mineloot_base.Treasury_evt_VaultReceived
  GROUP BY 1
),
buybacks AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    SUM(CAST(ethSpent AS DOUBLE) / 1e18) AS buyback_spent_eth,
    SUM(CAST(lootBurned AS DOUBLE) / 1e18) AS buyback_burned_loot,
    SUM(CAST(lootToStakers AS DOUBLE) / 1e18) AS staker_yield_loot
  FROM mineloot_base.Treasury_evt_BuybackExecuted
  GROUP BY 1
),
direct_burns AS (
  SELECT
    DATE_TRUNC('day', t.evt_block_time) AS day,
    SUM(CAST(t.value AS DOUBLE) / 1e18) AS direct_burned_loot
  FROM mineloot_base.Loot_evt_Transfer t
  LEFT JOIN mineloot_base.Treasury_evt_BuybackExecuted b
    ON b.evt_tx_hash = t.evt_tx_hash
  WHERE t."to" = 0x0000000000000000000000000000000000000000
    AND b.evt_tx_hash IS NULL
  GROUP BY 1
),
lock_rewards AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    SUM(CAST(amount AS DOUBLE) / 1e18) AS lock_rewards_eth
  FROM mineloot_base.LockerRewards_evt_RewardNotified
  GROUP BY 1
),
emission AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    COUNT(*) * 1.0 AS emitted_loot
  FROM mineloot_base.GridMining_evt_RoundSettled
  GROUP BY 1
),
days AS (
  SELECT day FROM revenue
  UNION SELECT day FROM buybacks
  UNION SELECT day FROM direct_burns
  UNION SELECT day FROM lock_rewards
  UNION SELECT day FROM emission
)
SELECT
  d.day,
  COALESCE(r.revenue_eth, 0) AS revenue_eth,
  COALESCE(b.buyback_spent_eth, 0) AS buyback_spent_eth,
  COALESCE(b.buyback_burned_loot, 0) + COALESCE(db.direct_burned_loot, 0) AS total_burned_loot,
  COALESCE(e.emitted_loot, 0) AS emitted_loot,
  CASE
    WHEN COALESCE(e.emitted_loot, 0) > 0
      THEN (COALESCE(b.buyback_burned_loot, 0) + COALESCE(db.direct_burned_loot, 0)) / e.emitted_loot
    ELSE NULL
  END AS burn_to_emission_ratio,
  COALESCE(b.staker_yield_loot, 0) AS staker_yield_loot,
  COALESCE(l.lock_rewards_eth, 0) AS lock_rewards_eth
FROM days d
LEFT JOIN revenue r ON r.day = d.day
LEFT JOIN buybacks b ON b.day = d.day
LEFT JOIN direct_burns db ON db.day = d.day
LEFT JOIN lock_rewards l ON l.day = d.day
LEFT JOIN emission e ON e.day = d.day
ORDER BY d.day DESC
LIMIT 365;

-- ===================================================================
-- Q10) Agent wallets summary (Atlas, Pulse, Vault, Shadow, Drift)
-- Mirrors API idea: rounds played, wins, deploy, est ETH reward, est ETH pnl
-- ===================================================================
WITH agent_wallets AS (
  SELECT * FROM (
    VALUES
      ('Atlas',  0xa35c47f491bee42777d77c3e47246c9b9af12981),
      ('Pulse',  0xba9182772915b5f6cac91b8834208cb32cbc8487),
      ('Vault',  0xc6ea08a824778f66d6b8ee783b2a65492a8157a3),
      ('Shadow', 0x71565c966001ccca0dea464e99b06a2c2c82e3bd),
      ('Drift',  0x66e493d19ac05c321982c2b8b164b4369073e006)
  ) AS t(agent_name, wallet)
),
deploys AS (
  SELECT evt_block_time, roundId, user, amountPerBlock, blockMask, totalAmount
  FROM mineloot_base.GridMining_evt_Deployed
  WHERE user IN (SELECT wallet FROM agent_wallets)
  UNION ALL
  SELECT evt_block_time, roundId, user, amountPerBlock, blockMask, totalAmount
  FROM mineloot_base.GridMining_evt_DeployedFor
  WHERE user IN (SELECT wallet FROM agent_wallets)
),
rounds AS (
  SELECT
    roundId,
    winningBlock,
    totalDeployed,
    totalWinnings,
    topMinerReward,
    lootpotAmount,
    isSplit,
    winnersDeployed,
    topMiner
  FROM mineloot_base.GridMining_evt_RoundSettled
),
joined AS (
  SELECT
    a.agent_name,
    d.user,
    d.roundId,
    d.evt_block_time,
    CAST(d.amountPerBlock AS DECIMAL(38, 0)) AS amount_per_block_wei,
    CAST(d.totalAmount AS DECIMAL(38, 0)) AS total_amount_wei,
    CAST(r.totalDeployed AS DECIMAL(38, 0)) AS total_deployed_wei,
    CAST(r.totalWinnings AS DECIMAL(38, 0)) AS total_winnings_wei,
    CAST(r.topMinerReward AS DECIMAL(38, 0)) AS top_miner_reward_wei,
    CAST(r.lootpotAmount AS DECIMAL(38, 0)) AS lootpot_amount_wei,
    CAST(r.winnersDeployed AS DECIMAL(38, 0)) AS winners_deployed_wei,
    CAST(r.winningBlock AS INTEGER) AS winning_block_zero_index,
    r.isSplit,
    r.topMiner,
    CASE
      WHEN bitwise_and(
        CAST(d.blockMask AS BIGINT),
        bitwise_left_shift(CAST(1 AS BIGINT), CAST(r.winningBlock AS INTEGER))
      ) > 0
      THEN 1
      ELSE 0
    END AS is_winner
  FROM deploys d
  JOIN rounds r
    ON r.roundId = d.roundId
  JOIN agent_wallets a
    ON a.wallet = d.user
),
calc AS (
  SELECT
    *,
    CAST(total_deployed_wei * 100 / 10000 AS DECIMAL(38, 0)) AS admin_fee_wei,
    CAST((total_deployed_wei - winners_deployed_wei) AS DECIMAL(38, 0)) AS losers_pool_wei,
    CAST((total_deployed_wei - winners_deployed_wei) * 100 / 10000 AS DECIMAL(38, 0)) AS losers_admin_wei
  FROM joined
),
rewards AS (
  SELECT
    *,
    CASE
      WHEN winners_deployed_wei = 0 AND total_deployed_wei > 0 AND total_winnings_wei = 0
        THEN total_deployed_wei - admin_fee_wei
      ELSE ((losers_pool_wei - losers_admin_wei) * 1000 / 10000)
    END AS vaulted_wei
  FROM calc
),
final_rows AS (
  SELECT
    *,
    (total_deployed_wei - admin_fee_wei - vaulted_wei) AS claimable_pool_wei,
    CASE
      WHEN is_winner = 1 AND winners_deployed_wei > 0
        THEN ((total_deployed_wei - admin_fee_wei - vaulted_wei) * amount_per_block_wei) / winners_deployed_wei
      ELSE CAST(0 AS DECIMAL(38, 0))
    END AS est_eth_reward_wei
  FROM rewards
)
SELECT
  agent_name,
  CONCAT('0x', LOWER(TO_HEX(user))) AS wallet,
  COUNT(DISTINCT roundId) AS rounds_played,
  SUM(is_winner) AS wins,
  COUNT(*) - SUM(is_winner) AS losses,
  CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * SUM(is_winner) / COUNT(*), 2) ELSE 0 END AS win_rate_pct,
  CAST(SUM(total_amount_wei) AS DOUBLE) / 1e18 AS total_deployed_eth,
  CAST(SUM(est_eth_reward_wei) AS DOUBLE) / 1e18 AS est_total_rewards_eth,
  CAST(SUM(est_eth_reward_wei - total_amount_wei) AS DOUBLE) / 1e18 AS est_eth_pnl,
  MAX(evt_block_time) AS last_active_at
FROM final_rows
GROUP BY 1, 2
ORDER BY total_deployed_eth DESC;

-- ===================================================================
-- Q11) Agent wallet treasury actions (burn vs rebalance)
-- Uses LOOT Transfer from each agent wallet.
-- ===================================================================
WITH agent_wallets AS (
  SELECT * FROM (
    VALUES
      ('Atlas',  0xa35c47f491bee42777d77c3e47246c9b9af12981),
      ('Pulse',  0xba9182772915b5f6cac91b8834208cb32cbc8487),
      ('Vault',  0xc6ea08a824778f66d6b8ee783b2a65492a8157a3),
      ('Shadow', 0x71565c966001ccca0dea464e99b06a2c2c82e3bd),
      ('Drift',  0x66e493d19ac05c321982c2b8b164b4369073e006)
  ) AS t(agent_name, wallet)
)
SELECT
  a.agent_name,
  CASE
    WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN 'burn'
    ELSE 'rebalance'
  END AS action_type,
  COUNT(*) AS tx_count,
  CAST(SUM(t.value) AS DOUBLE) / 1e18 AS loot_amount
FROM mineloot_base.Loot_evt_Transfer t
JOIN agent_wallets a
  ON a.wallet = t."from"
GROUP BY 1, 2
ORDER BY 1, 2;

-- ===================================================================
-- Q12) Agent rounds detail (last 200 rounds involving agent wallets)
-- ===================================================================
WITH agent_wallets AS (
  SELECT * FROM (
    VALUES
      ('Atlas',  0xa35c47f491bee42777d77c3e47246c9b9af12981),
      ('Pulse',  0xba9182772915b5f6cac91b8834208cb32cbc8487),
      ('Vault',  0xc6ea08a824778f66d6b8ee783b2a65492a8157a3),
      ('Shadow', 0x71565c966001ccca0dea464e99b06a2c2c82e3bd),
      ('Drift',  0x66e493d19ac05c321982c2b8b164b4369073e006)
  ) AS t(agent_name, wallet)
),
deploys AS (
  SELECT evt_block_time, roundId, user, amountPerBlock, blockMask, totalAmount, evt_tx_hash
  FROM mineloot_base.GridMining_evt_Deployed
  WHERE user IN (SELECT wallet FROM agent_wallets)
  UNION ALL
  SELECT evt_block_time, roundId, user, amountPerBlock, blockMask, totalAmount, evt_tx_hash
  FROM mineloot_base.GridMining_evt_DeployedFor
  WHERE user IN (SELECT wallet FROM agent_wallets)
),
settled AS (
  SELECT roundId, winningBlock, lootpotAmount, isSplit
  FROM mineloot_base.GridMining_evt_RoundSettled
)
SELECT
  d.evt_block_time,
  a.agent_name,
  CONCAT('0x', LOWER(TO_HEX(d.user))) AS wallet,
  d.roundId AS round_id,
  CAST(s.winningBlock AS INTEGER) + 1 AS winning_block,
  CAST(d.totalAmount AS DOUBLE) / 1e18 AS deployed_eth,
  CASE
    WHEN bitwise_and(
      CAST(d.blockMask AS BIGINT),
      bitwise_left_shift(CAST(1 AS BIGINT), CAST(s.winningBlock AS INTEGER))
    ) > 0 THEN 'Win'
    ELSE 'Miss'
  END AS outcome,
  CASE WHEN CAST(s.lootpotAmount AS DECIMAL(38, 0)) > 0 THEN true ELSE false END AS lootpot_hit,
  s.isSplit AS split_round,
  d.evt_tx_hash
FROM deploys d
JOIN settled s
  ON s.roundId = d.roundId
JOIN agent_wallets a
  ON a.wallet = d.user
ORDER BY d.roundId DESC, a.agent_name
LIMIT 200;

-- ===================================================================
-- Q13) Lootpot + split cadence (daily)
-- Matches API/copilot historical context.
-- ===================================================================
SELECT
  DATE_TRUNC('day', evt_block_time) AS day,
  COUNT(*) AS settled_rounds,
  SUM(CASE WHEN CAST(lootpotAmount AS DECIMAL(38, 0)) > 0 THEN 1 ELSE 0 END) AS lootpot_hits,
  SUM(CASE WHEN isSplit THEN 1 ELSE 0 END) AS split_rounds,
  CAST(SUM(CAST(lootpotAmount AS DOUBLE) / 1e18) AS DOUBLE) AS lootpot_distributed_loot
FROM mineloot_base.GridMining_evt_RoundSettled
GROUP BY 1
ORDER BY 1 DESC
LIMIT 365;

-- ===================================================================
-- Q14) LP Fees Burn wallet tracker
-- Special treasury burn wallet used by protocol ops.
-- ===================================================================
SELECT
  DATE_TRUNC('day', evt_block_time) AS day,
  COUNT(*) AS burn_txs,
  CAST(SUM(value) AS DOUBLE) / 1e18 AS burned_loot
FROM mineloot_base.Loot_evt_Transfer
WHERE "from" = 0x7da1539189fec6f12d80df395f27604debce5dc0
  AND "to" = 0x0000000000000000000000000000000000000000
GROUP BY 1
ORDER BY 1 DESC
LIMIT 365;

-- ===================================================================
-- Q15) Staking health (total staked, yield distributed, TVL proxy)
-- ===================================================================
WITH deposits AS (
  SELECT user, CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.Staking_evt_Deposited
),
withdrawals AS (
  SELECT user, -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.Staking_evt_Withdrawn
),
balances AS (
  SELECT
    user,
    COALESCE(SUM(delta_wei), 0) AS staked_wei
  FROM (
    SELECT * FROM deposits
    UNION ALL
    SELECT * FROM withdrawals
  ) s
  GROUP BY 1
),
totals AS (
  SELECT
    COALESCE(SUM(CASE WHEN staked_wei > 0 THEN staked_wei ELSE 0 END), 0) AS total_staked_wei,
    COUNT_IF(staked_wei > 0) AS active_stakers
  FROM balances
),
yield_dist AS (
  SELECT COALESCE(SUM(amount), 0) AS total_yield_wei
  FROM mineloot_base.Staking_evt_YieldDistributed
)
SELECT
  CAST(t.total_staked_wei AS DOUBLE) / 1e18 AS total_staked_loot,
  CAST(y.total_yield_wei AS DOUBLE) / 1e18 AS total_yield_distributed_loot,
  t.active_stakers
FROM totals t
CROSS JOIN yield_dist y;

-- ===================================================================
-- Q16) Lock health (locked supply, weight, notified/claimed, lockers)
-- ===================================================================
WITH deltas AS (
  SELECT user, CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_Locked
  UNION ALL
  SELECT user, CAST(amountAdded AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_AddedToLock
  UNION ALL
  SELECT user, -CAST(amount AS DECIMAL(38, 0)) AS delta_wei
  FROM mineloot_base.LootLocker_evt_Unlocked
),
locked_by_user AS (
  SELECT user, COALESCE(SUM(delta_wei), 0) AS locked_wei
  FROM deltas
  GROUP BY 1
),
totals AS (
  SELECT
    COALESCE(SUM(CASE WHEN locked_wei > 0 THEN locked_wei ELSE 0 END), 0) AS protocol_locked_wei,
    COUNT_IF(locked_wei > 0) AS lockers
  FROM locked_by_user
),
weights AS (
  SELECT
    COALESCE(MAX(newTotalWeight), 0) AS protocol_weight_wei
  FROM (
    SELECT newTotalWeight, evt_block_time FROM mineloot_base.LootLocker_evt_Locked
    UNION ALL
    SELECT newTotalWeight, evt_block_time FROM mineloot_base.LootLocker_evt_AddedToLock
    UNION ALL
    SELECT newTotalWeight, evt_block_time FROM mineloot_base.LootLocker_evt_Extended
    UNION ALL
    SELECT newTotalWeight, evt_block_time FROM mineloot_base.LootLocker_evt_Unlocked
  ) t
),
notified AS (
  SELECT COALESCE(SUM(amount), 0) AS total_notified_wei
  FROM mineloot_base.LockerRewards_evt_RewardNotified
)
SELECT
  CAST(t.protocol_locked_wei AS DOUBLE) / 1e18 AS protocol_locked_loot,
  CAST(w.protocol_weight_wei AS DOUBLE) / 1e18 AS protocol_weight,
  CAST(n.total_notified_wei AS DOUBLE) / 1e18 AS total_notified_eth,
  CAST(NULL AS DOUBLE) AS total_claimed_eth,
  t.lockers
FROM totals t
CROSS JOIN weights w
CROSS JOIN notified n

-- Q16b) Optional: run only if this table exists in your decoded schema.
-- SELECT
--   CAST(SUM(amount) AS DOUBLE) / 1e18 AS total_claimed_eth
-- FROM mineloot_base.LockerRewards_evt_RewardClaimed;
