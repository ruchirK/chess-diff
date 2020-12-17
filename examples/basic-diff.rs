use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use differential_dataflow::input::Input;
use differential_dataflow::operators::{CountTotal, Join, Threshold};
use differential_dataflow::Collection;

fn main() {
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

    timely::execute_directly(move |worker| {
        let mut input = worker.dataflow(|scope| {
            let (input_handle, input): (_, Collection<_, Vec<String>, isize>) =
                scope.new_collection();
            /*
                        let first = input.filter(|s| s[0] == "White-Knight-G").map(|mut s| {
                            s.remove(0);
                            s
                        });
                        let second = input.filter(|s| s[0] == "White-Knight-B").map(|mut s| {
                            s.remove(0);
                            s
                        });
            */

            let first = input.filter(|s| s[2] == "e5").map(|mut s| {
                s.remove(2);
                s
            });
            let second = input.filter(|s| s[2] == "e4").map(|mut s| {
                s.remove(2);
                s
            });

            let get_subsets = move |x: Vec<String>| {
                // XXX: I don't really understand the ramifications of line 24
                let subsets = subsets.clone();
                subsets
                    .into_iter()
                    .map(move |(first, second, third, fourth)| {
                        let mut output: Vec<Option<String>> = vec![None; 4];
                        if first == 1 {
                            output[0] = Some(x[0].clone());
                        }
                        if second == 1 {
                            output[1] = Some(x[1].clone());
                        }
                        if third == 1 {
                            output[2] = Some(x[2].clone());
                        }

                        if fourth == 1 {
                            output[3] = Some(x[3].clone());
                        }
                        output
                    })
            };

            // XXX: Terrible
            let get_subsets_second = get_subsets.clone();

            let first_rule_counts = first.flat_map(move |x| get_subsets(x)).count_total();

            let second_rule_counts = second
                .flat_map(move |x| get_subsets_second(x))
                .count_total();

            //first_cubed_counts.inspect(|(x, time, m)| println!("x: {:?} time: {} multiplicity: {}", x, time, m));
            let first_total_count = first.map(|_| ()).count_total();

            let second_total_count = second.map(|_| ()).count_total();

            let first_counts = first_rule_counts
                .map(|x| ((), x))
                .join(&first_total_count)
                .map(|(_, ((rule, rule_count), total))| (rule, (rule_count, total)));

            //first_counts.inspect(|(x, time, m)| println!("[first] x: {:?} time: {} multiplicity: {}", x, time, m));

            let second_counts = second_rule_counts
                .map(|x| ((), x))
                .join(&second_total_count)
                .map(|(_, ((rule, rule_count), total))| (rule, (rule_count, total)));

            //second_counts.inspect(|(x, time, m)| println!("[second] x: {:?} time: {} multiplicity: {}", x, time, m));

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
                    let out: Vec<String> =
                        rule.into_iter().map(|x| x.unwrap_or("*".into())).collect();
                    (out, support_a, risk_ratio)
                })
                .inspect(|((x, support, risk_ratio), _, _)| {
                    println!("[rule]: {:?} {:.2}% {:.2}", x, *support * 100.0, risk_ratio)
                });
            input_handle
        });

        input.advance_to(0);
        let mut count = 0;
        for arg in env::args().skip(1) {
            let file = File::open(&arg).expect("fopen");

            let reader = BufReader::new(file);
            for line in reader.lines() {
                let l: Vec<String> = line.unwrap().split(',').map(|s| s.to_string()).collect();

                if l.len() != 5 {
                    println!("{:?}", l);
                }
                input.insert(l);
                count = count + 1;

                if count % 10000 == 0 {
                    //input.advance_to(count);
                    println!("[input-count]: {}", count);
                }
            }
        }
    })
}
