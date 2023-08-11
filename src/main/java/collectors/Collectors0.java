package collectors;

import data.Singleton;

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
}
