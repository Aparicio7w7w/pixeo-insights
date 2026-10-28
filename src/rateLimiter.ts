interface RateBucket {
  count: number;
  resetAt: number;
}

export class FixedWindowRateLimiter {
  private readonly buckets = new Map<string, RateBucket>();
  private requestsSinceSweep = 0;

  public constructor(
    private readonly limit: number,
    private readonly windowMs = 60_000,
  ) {}

  public consume(key: string, now = Date.now()): boolean {
    this.requestsSinceSweep += 1;
    if (this.requestsSinceSweep >= 1000) {
      this.sweep(now);
    }

    const current = this.buckets.get(key);
    if (!current || current.resetAt <= now) {
      this.buckets.set(key, { count: 1, resetAt: now + this.windowMs });
      return true;
    }

    if (current.count >= this.limit) {
      return false;
    }

    current.count += 1;
    return true;
  }

  private sweep(now: number): void {
    for (const [key, bucket] of this.buckets.entries()) {
      if (bucket.resetAt <= now) {
        this.buckets.delete(key);
      }
    }
    this.requestsSinceSweep = 0;
  }
}
