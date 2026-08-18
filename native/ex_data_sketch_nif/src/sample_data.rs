use rand::Rng;

// Generators for livebooks/sketches/*.livemd sample data (see
// lib/ex_data_sketch/sample_data.ex). Not part of the sketch algorithms --
// these exist purely to make generating 1-2 million demo items fast. Output
// is not required to match the Pure Elixir fallback bit-for-bit (unlike the
// sketch backends), only to have the same distributional shape.
//
// Each NIF is a thin wrapper around a plain `_impl` function (mirroring the
// rest of this crate's convention) so the generation logic is callable
// directly from `#[cfg(test)]` without going through rustler's NIF export
// machinery.

/// `count` strings of the form "{prefix}{idx}", idx in 1..pool_size.
/// exponent = 1.0 gives a uniform pool (HLL/ULL); exponent > 1.0 skews
/// low indices to appear far more often (a power-law / Zipfian shape).
fn string_events_impl(prefix: &str, count: u64, pool_size: u64, exponent: f64) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let span = (pool_size.max(1) - 1) as f64;

    (0..count)
        .map(|_| {
            let idx = (rng.gen::<f64>().powf(exponent) * span).trunc() as u64 + 1;
            format!("{prefix}{idx}")
        })
        .collect()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn sample_data_string_events_nif(
    prefix: String,
    count: u64,
    pool_size: u64,
    exponent: f64,
) -> Vec<String> {
    string_events_impl(&prefix, count, pool_size, exponent)
}

/// KLL tutorial: mostly-fast latencies (ms) with an occasional long-tail spike.
fn kll_latencies_impl(count: u64) -> Vec<f64> {
    let mut rng = rand::thread_rng();

    (0..count)
        .map(|_| {
            let base = rng.gen::<f64>() * rng.gen::<f64>() * 200.0;
            if rng.gen_range(1..=100) == 1 {
                base + rng.gen::<f64>() * 2000.0
            } else {
                base
            }
        })
        .collect()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn sample_data_kll_latencies_nif(count: u64) -> Vec<f64> {
    kll_latencies_impl(count)
}

/// DDSketch tutorial: durations (ms) spanning several orders of magnitude
/// across three tiers (fast API calls, medium DB queries, rare slow jobs).
fn ddsketch_durations_impl(count: u64) -> Vec<f64> {
    let mut rng = rand::thread_rng();

    (0..count)
        .map(|_| match rng.gen_range(1..=1000) {
            n if n <= 900 => rng.gen::<f64>() * 50.0,
            n if n <= 990 => 50.0 + rng.gen::<f64>() * 450.0,
            _ => 10_000.0 + rng.gen::<f64>() * 90_000.0,
        })
        .collect()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn sample_data_ddsketch_durations_nif(count: u64) -> Vec<f64> {
    ddsketch_durations_impl(count)
}

/// REQ tutorial: tight, boring bulk latencies (ms) with a rare, important tail.
fn req_latencies_impl(count: u64) -> Vec<f64> {
    let mut rng = rand::thread_rng();

    (0..count)
        .map(|_| {
            if rng.gen_range(1..=1000) == 1 {
                500.0 + rng.gen::<f64>() * 4500.0
            } else {
                10.0 + rng.gen::<f64>() * 20.0
            }
        })
        .collect()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn sample_data_req_latencies_nif(count: u64) -> Vec<f64> {
    req_latencies_impl(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_index(item: &str, prefix: &str) -> u64 {
        item.strip_prefix(prefix)
            .unwrap_or_else(|| panic!("{item:?} missing prefix {prefix:?}"))
            .parse()
            .unwrap_or_else(|_| panic!("{item:?} has a non-numeric suffix"))
    }

    #[test]
    fn string_events_returns_exactly_count_items() {
        let events = string_events_impl("visitor_", 500, 50, 1.0);
        assert_eq!(events.len(), 500);
    }

    #[test]
    fn string_events_are_prefixed_with_an_index_in_bounds() {
        let events = string_events_impl("session_", 300, 30, 1.0);
        for item in &events {
            let idx = parse_index(item, "session_");
            assert!(
                (1..=30).contains(&idx),
                "{idx} out of bounds for pool_size 30"
            );
        }
    }

    #[test]
    fn string_events_uniform_exponent_covers_most_of_the_pool() {
        let events = string_events_impl("page_", 5000, 50, 1.0);
        let distinct: std::collections::HashSet<u64> =
            events.iter().map(|e| parse_index(e, "page_")).collect();
        // 5000 uniform draws over a 50-item pool should hit nearly every
        // index; a real bug (off-by-one range, wrong RNG usage) would show
        // up as a narrower-than-expected spread.
        assert!(
            distinct.len() >= 45,
            "only {} of 50 indices were drawn",
            distinct.len()
        );
    }

    #[test]
    fn string_events_power_law_exponent_skews_toward_low_indices() {
        let events = string_events_impl("query_", 5000, 100, 3.0);
        let indices: Vec<u64> = events.iter().map(|e| parse_index(e, "query_")).collect();
        let low_half = indices.iter().filter(|&&i| i <= 50).count();
        // A uniform draw would put ~50% of the mass in the low half; the
        // power-law (exponent 3.0) shape used by the CQF/FrequentItems/
        // MisraGries tutorials should concentrate far more than that.
        assert!(
            low_half as f64 / 5000.0 > 0.65,
            "power-law draw wasn't skewed: {low_half}/5000 in the low half"
        );
    }

    #[test]
    fn string_events_pool_size_one_never_panics() {
        let events = string_events_impl("x_", 10, 1, 1.0);
        assert_eq!(events, vec!["x_1".to_string(); 10]);
    }

    #[test]
    fn kll_latencies_returns_count_non_negative_values() {
        let values = kll_latencies_impl(1000);
        assert_eq!(values.len(), 1000);
        assert!(values.iter().all(|&v| v >= 0.0));
    }

    #[test]
    fn ddsketch_durations_returns_count_non_negative_values_spanning_tiers() {
        let values = ddsketch_durations_impl(2000);
        assert_eq!(values.len(), 2000);
        assert!(values.iter().all(|&v| v >= 0.0));
        let max = values.iter().cloned().fold(0.0_f64, f64::max);
        // The rare (~1%) slow-job tier reaches into the tens of thousands;
        // at n=2000 it should show up and dwarf the fast-tier bulk.
        assert!(
            max > 1000.0,
            "expected the slow-job tier to appear, max was {max}"
        );
    }

    #[test]
    fn req_latencies_returns_count_non_negative_values_mostly_in_the_tight_bulk() {
        let values = req_latencies_impl(2000);
        assert_eq!(values.len(), 2000);
        assert!(values.iter().all(|&v| v >= 0.0));
        let bulk = values.iter().filter(|&&v| v < 30.0).count();
        assert!(
            bulk as f64 / 2000.0 > 0.9,
            "expected >90% of values under 30ms, got {bulk}/2000"
        );
    }
}
