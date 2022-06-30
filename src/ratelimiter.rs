// TODO: figure out how to add jitter support using `governor::Jitter`.
// TODO: add usage examples (both in the docs and in an examples directory).
// TODO: add unit tests.
use crate::args::RateLimits;
use anyhow::Result;
use governor::{
    clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota,
    RateLimiter,
};
use lazy_static::lazy_static;
use std::{
    collections::HashMap, convert::TryInto, error::Error, num::NonZeroU32,
    sync::Arc, time::Duration,
};

lazy_static! {
    static ref CLOCK: DefaultClock = DefaultClock::default();
}

/// Once the rate limit has been reached, the middleware will respond with
/// status code 429 (too many requests) and a `Retry-After` header with the amount
/// of time that needs to pass before another request will be allowed.
#[derive(Debug, Clone)]
pub struct GovernorLogRateLimiter {
    limiter:
        Arc<RateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>>,
}

impl GovernorLogRateLimiter {
    /// Constructs a rate-limiting middleware from a [`Duration`] that allows one request in the given time interval.
    ///
    /// If the time interval is zero, returns `None`.
    #[must_use]
    pub fn with_period(duration: Duration) -> Option<Self> {
        Some(Self {
            limiter: Arc::new(RateLimiter::<String, _, _>::keyed(
                Quota::with_period(duration)?,
            )),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every second.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_second<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<String, _, _>::keyed(
                Quota::per_second(times.try_into()?),
            )),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every minute.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_minute<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<String, _, _>::keyed(
                Quota::per_minute(times.try_into()?),
            )),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every hour.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_hour<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<String, _, _>::keyed(
                Quota::per_hour(times.try_into()?),
            )),
        })
    }

    //    fn check_key(&self, key: &String) -> Result<()> {
    fn check_key(&self, key: &str) -> Result<()> {
        // TODO: remove &str to &String copy overhead
        self.limiter
            .check_key(&key.to_string())
            .map_err(|err| anyhow::anyhow!("error: {err}"))
    }
}

pub struct RateLimitMap {
    map: HashMap<String, GovernorLogRateLimiter>,
}

impl RateLimitMap {
    pub fn from_config(config: Option<RateLimits>) -> Option<RateLimitMap> {
        config.map(|rate_limits| {
            let mut map = HashMap::new();
            for (app, rate) in rate_limits.rate_limits.iter() {
                map.insert(
                    app.to_string(),
                    GovernorLogRateLimiter::per_second(*rate).unwrap(),
                );
            }
            RateLimitMap { map }
        })
    }

    pub fn init() -> RateLimitMap {
        let mut map = HashMap::new();
        map.insert(
            "app-titan".to_string(),
            GovernorLogRateLimiter::per_second(10).unwrap(),
        );
        RateLimitMap { map }
    }

    pub fn check_key(&self, key: &str) -> Result<()> {
        match self.map.get(key) {
            Some(ratelimit) => ratelimit.check_key(key),
            None => Ok(()),
        }
    }
}
