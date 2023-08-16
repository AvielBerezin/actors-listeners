package collectors;

import data.Singleton;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;

public class Collectors0 {
    public static <T> Collector<T, Singleton, Singleton> ignoring() {
        return Collector.of(() -> Singleton.INSTANCE,
                            (acc, e) -> {},
                            (s1, s2) -> Singleton.INSTANCE);
    }

    public static <Elem> Collector<Elem, AtomicBoolean, Boolean> exists() {
        return Collector.of(
                AtomicBoolean::new,
                (acc, x) -> acc.set(true),
                (b1, b2) -> {
                    if (b2.get()) {
                        b1.set(true);
                    }
                    return b1;
                },
                AtomicBoolean::get);
    }

    public record Pair<F, S>(F fst, S snd) {}

    public static <Elem, Acc, Result, PredicateAcc>
    Collector<Elem, Pair<Acc, PredicateAcc>, Optional<Result>> filter(Collector<Elem, Acc, Result> collector,
                                                                      Collector<Elem, PredicateAcc, Boolean> predicate) {
        return Collector.of(() -> new Pair<>(collector.supplier().get(), predicate.supplier().get()),
                            (accs, elem) -> {
                                collector.accumulator().accept(accs.fst, elem);
                                predicate.accumulator().accept(accs.snd, elem);
                            },
                            (accs1, accs2) -> new Pair<>(collector.combiner().apply(accs1.fst, accs2.fst),
                                                         predicate.combiner().apply(accs1.snd, accs2.snd)),
                            accs -> predicate.finisher().apply(accs.snd)
                                    ? Optional.of(collector.finisher().apply(accs.fst))
                                    : Optional.empty());
    }
}
