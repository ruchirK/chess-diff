use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader};

use differential_dataflow::input::Input;
use differential_dataflow::operators::{CountTotal, Join, Threshold};
use differential_dataflow::Collection;

pub struct SpaceSaving<T> {
    keys: HashMap<Vec<T>, (usize, usize)>,
    counts: BTreeSet<(usize, Vec<T>)>,
    limit: usize,
    size: usize,
}

impl<T> SpaceSaving<T>
where
    T: Ord + Eq + Hash + Clone,
{
    fn new(limit: usize) -> Self {
        SpaceSaving {
            keys: HashMap::new(),
            counts: BTreeSet::new(),
            limit,
            size: 0,
        }
    }

    fn insert(&mut self, key: Vec<T>) {
        if let Some(val) = self.keys.get_mut(&key) {
            let old_count = val.0;
            val.0 += 1;

            let mut old_tuple = (old_count, key);
            let cleanup = self.counts.remove(&old_tuple);
            assert!(cleanup == true);

            old_tuple.0 += 1;
            self.counts.insert(old_tuple);
        } else {
            if self.size < self.limit {
                // Easy case, we have not yet filled up our quota of counters
                self.size += 1;

                self.keys.insert(key.clone(), (1, 0));
                self.counts.insert((1, key));
            } else {
                // Ok now we have to evict one of our neighbors
                assert!(self.size == self.limit);

                // First, lets find the neighbor with the minimum count
                let min = self.counts.iter().next().unwrap().clone();
                let removal = self.counts.remove(&min);
                assert!(removal == true);
                self.keys.remove(&min.1).unwrap();

                // Now let's insert the new key with a nonzero error term
                let count = min.0 + 1;
                let error = min.0;
                self.keys.insert(key.clone(), (count, error));
                self.counts.insert((count, key));
            }
        }
    }

    fn get_counts(&self) -> Vec<(Vec<T>, (usize, usize))> {
        self.keys
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

fn main() {
    timely::execute_directly(move |worker| {
        let (mut first_rule_counts, mut second_rule_counts) = worker.dataflow(|scope| {
            // (Rule, (rule_count, total_count))
            let (first_handle, first_counts): (
                _,
                Collection<_, (Vec<String>, (usize, usize)), isize>,
            ) = scope.new_collection();
            let (second_handle, second_counts): (
                _,
                Collection<_, (Vec<String>, (usize, usize)), isize>,
            ) = scope.new_collection();

            // Compute risk ratios
            let common_rules = first_counts.join(&second_counts).map(
                |(
                    rule,
                    (
                        (first_rule_count, first_total_count),
                        (second_rule_count, second_total_count),
                    ),
                )| {
                    let p_rule =
                        first_rule_count as f64 / (first_rule_count + second_rule_count) as f64;

                    let total_without_rule = (first_total_count - first_rule_count
                        + second_total_count
                        - second_rule_count) as f64;
                    let first_without_rule = (first_total_count - first_rule_count) as f64;
                    let first_support = first_rule_count as f64 / first_total_count as f64;
                    let second_support = second_rule_count as f64 / second_total_count as f64;

                    if total_without_rule == 0.0 {
                        (rule, first_support, second_support, 0.0)
                    } else if first_without_rule == 0.0 {
                        (rule, first_support, second_support, f64::INFINITY)
                    } else {
                        let p_without_rule = first_without_rule / total_without_rule;
                        (rule, first_support, second_support, p_rule / p_without_rule)
                    }
                },
            );

            let second_total_count = second_counts.map(|(_, (_, total))| ((), total)).distinct();
            let counts_not_in_second = first_counts
                .map(|(k, _)| k)
                .distinct()
                .concat(&common_rules.map(|(k, _, _, _)| k).distinct().negate())
                .map(|x| ((), x))
                .join(&second_total_count)
                .map(|(_, (rule, total))| (rule, (0, total)));

            let rules_only_in_first = first_counts.join(&counts_not_in_second).map(
                |(rule, ((first_rule_count, first_total_count), (_, second_total_count)))| {
                    let p_rule = 1.0 as f64;

                    let total_without_rule =
                        (first_total_count - first_rule_count + second_total_count) as f64;
                    let first_without_rule = (first_total_count - first_rule_count) as f64;
                    let first_support = first_rule_count as f64 / first_total_count as f64;
                    let second_support = 0.0 as f64;

                    if total_without_rule == 0.0 {
                        (rule, first_support, second_support, 0.0)
                    } else if first_without_rule == 0.0 {
                        (rule, first_support, second_support, f64::INFINITY)
                    } else {
                        let p_without_rule = first_without_rule / total_without_rule;
                        (rule, first_support, second_support, p_rule / p_without_rule)
                    }
                },
            );

            let rules = common_rules.concat(&rules_only_in_first);

            rules
                .filter(|(_, support, _, risk_ratio)| *support > 0.05 && *risk_ratio > 1.2)
                .map(|(rule, support_a, _, risk_ratio)| {
                    let out: Vec<String> = rule.into_iter().collect();
                    (out, support_a, risk_ratio)
                })
                .inspect(|((x, support, risk_ratio), _, _)| {
                    println!("[rule]: {:?} {:.2}% {:.2}", x, *support * 100.0, risk_ratio)
                });
            (first_handle, second_handle)
        });

        let subsets = vec![
            (0, 0, 0, 1),
            (0, 0, 1, 0),
            (0, 0, 1, 1),
            (0, 1, 0, 0),
            (0, 1, 0, 1),
            (0, 1, 1, 0),
            (0, 1, 1, 1),
            (1, 0, 0, 0),
            (1, 0, 0, 1),
            (1, 0, 1, 0),
            (1, 0, 1, 1),
            (1, 1, 0, 0),
            (1, 1, 0, 1),
            (1, 1, 1, 0),
        ];

        let mut first = Vec::new();
        let mut second = Vec::new();
        let mut first_count = 0;
        let mut second_count = 0;

        for arg in env::args().skip(1) {
            let file = File::open(&arg).expect("fopen");

            let reader = BufReader::new(file);
            let mut count = 0;
            for line in reader.lines() {
                let mut l: Vec<String> = line.unwrap().split(',').map(|s| s.to_string()).collect();

                if l.len() != 5 {
                    println!("{:?}", l);
                }
                count = count + 1;

                if count % 10000 == 0 {
                    println!("[input-count]: {}", count);
                }

                if l[2] == "e5" {
                    l.remove(2);
                    first.push(l);
                    first_count += 1;
                } else if l[2] == "e4" {
                    l.remove(2);
                    second.push(l);
                    second_count += 1;
                }
            }
        }
        let get_subsets = move |x: Vec<String>| {
            // XXX: I don't really understand the ramifications of line 24
            let subsets = subsets.clone();
            subsets
                .into_iter()
                .map(move |(first, second, third, fourth)| {
                    let mut output: Vec<String> = vec!["*".to_string(); 4];
                    if first == 1 {
                        output[0] = x[0].clone();
                    }
                    if second == 1 {
                        output[1] = x[1].clone();
                    }
                    if third == 1 {
                        output[2] = x[2].clone();
                    }

                    if fourth == 1 {
                        output[3] = x[3].clone();
                    }
                    output
                })
        };

        let counts_limit = 700;
        let mut counts_first = SpaceSaving::new(counts_limit);
        let mut counts_second = SpaceSaving::new(counts_limit);

        for f in first.into_iter() {
            for subset in get_subsets(f) {
                counts_first.insert(subset);
            }
        }

        for s in second.into_iter() {
            for subset in get_subsets(s) {
                counts_second.insert(subset);
            }
        }

        let first = counts_first.get_counts();
        let second = counts_second.get_counts();

        //println!("{:?}", first);

        let first_data: Vec<_> = first
            .into_iter()
            .map(|(rule, (rule_count, _))| (rule, (rule_count, first_count)))
            .collect();
        let second_data: Vec<_> = second
            .into_iter()
            .map(|(rule, (rule_count, _))| (rule, (rule_count, second_count)))
            .collect();

        first_rule_counts.advance_to(0);
        second_rule_counts.advance_to(0);

        for d in first_data.into_iter() {
            first_rule_counts.insert(d);
        }

        for d in second_data.into_iter() {
            second_rule_counts.insert(d);
        }

        //second_rule_counts.insert((vec!["dummy".to_string(); 5], (0, 0)));
    })
}
