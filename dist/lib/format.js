import { formatEther } from 'viem';
export function etherString(value) {
    return formatEther(value);
}
export function etherFixed(value, digits = 4) {
    return Number(formatEther(value)).toFixed(digits);
}
export function toBigInt(value) {
    if (typeof value === 'bigint')
        return value;
    if (typeof value === 'number')
        return BigInt(value);
    if (typeof value === 'string')
        return BigInt(value);
    throw new Error(`Unsupported bigint value: ${String(value)}`);
}
export function decodeBlockMask(mask) {
    const blocks = [];
    for (let i = 0; i < 25; i++) {
        if (((mask >> BigInt(i)) & 1n) === 1n)
            blocks.push(i);
    }
    return blocks;
}
export function countSelectedBlocks(mask) {
    return decodeBlockMask(mask).length;
}
export function safeAddressEq(a, b) {
    return !!a && !!b && a.toLowerCase() === b.toLowerCase();
}
export function relativeTime(timestampMs) {
    const seconds = Math.floor((Date.now() - timestampMs) / 1000);
    if (seconds < 0)
        return 'just now';
    if (seconds < 60)
        return `${seconds} sec ago`;
    if (seconds < 3600)
        return `${Math.floor(seconds / 60)} min ago`;
    if (seconds < 86400)
        return `${Math.floor(seconds / 3600)} hours ago`;
    return `${Math.floor(seconds / 86400)} days ago`;
}
