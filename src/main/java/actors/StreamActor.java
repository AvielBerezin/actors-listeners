package actors;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface StreamActor<Val, Err> {
    void perform(StreamListener<Val, Err> listener);

    default void perform() {
        perform(StreamListener.nop());
    }

    default void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    default StreamActor<Val, Err> withEach(Consumer<Val> onValue) {
        return listener -> perform(listener.withEach(onValue));
    }

    default StreamActor<Val, Err> withError(Consumer<Err> onError) {
        return listener -> perform(listener.withError(onError));
    }

    default StreamActor<Val, Err> withCompletion(Runnable onComplete) {
        return listener -> perform(listener.withCompletion(onComplete));
    }

    default StreamActor<Val, Err> anyway(Runnable onDone) {
        return listener -> perform(listener.anyway(onDone));
    }

    static <Val, Err> StreamActor<Val, Err> of(Collection<Val> collection) {
        return listener -> {
            for (Val val : collection) {
                listener = listener.onValue(val);
            }
            listener.onComplete();
        };
    }

    static <Val, Err> StreamActor<Val, Err> err(Err err) {
        return listener -> listener.onError(err);
    }

    default <ColVal, Acc> ValueActor<ColVal, Err> collect(Collector<Val, Acc, ColVal> collector) {
        return listener -> {
            Acc acc = collector.supplier().get();
            this.withEach(val -> collector.accumulator().accept(acc, val))
                .withError(listener::onError)
                .withCompletion(() -> listener.onValue(collector.finisher().apply(acc)))
                .perform();
        };
    }

    default <MVal> StreamActor<MVal, Err> map(Function<Val, MVal> mapper) {
//        return listener -> perform(listener.from(mapper));
        return flatMap(mapper.andThen(mVal -> StreamActor.of(List.of(mVal))),
                       Collector.of(AtomicReference<Err>::new,
                                    AtomicReference::set,
                                    (ar1, ar2) -> {
                                        if (ar1.get() == null) {
                                            return ar2;
                                        } else {
                                            return ar1;
                                        }
                                    },
                                    AtomicReference::get));
    }

    record ErrSummery<Err>(boolean occurred, Err err) {}

    default <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap(
            Function<Val, StreamActor<MVal, Err>> mapper,
            Collector<Err, ErrAcc, ColErr> errCollector) {
        Collector<Err, AtomicBoolean, Boolean> exists = Collector.of(
                AtomicBoolean::new,
                (acc, x) -> acc.set(true),
                (b1, b2) -> {
                    if (b2.get()) {
                        b1.set(true);
                    }
                    return b1;
                },
                AtomicBoolean::get);
        Collector<Err, ?, ErrSummery<ColErr>> errSummeryCollector = Collectors.teeing(exists, errCollector, ErrSummery::new);
        return flatMap0(mapper, errSummeryCollector);
    }

    private <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap0(
            Function<Val, StreamActor<MVal, Err>> mapper,
            Collector<Err, ErrAcc, ErrSummery<ColErr>> errSummeryCollector) {
        return listener -> {
            AtomicInteger processes = new AtomicInteger(1);
            AtomicReference<StreamListener<MVal, ColErr>> listenerHolder = new AtomicReference<>(listener);
            ErrAcc errAcc = errSummeryCollector.supplier().get();
            this.withEach(val -> processes.incrementAndGet())
                .withEach(val -> mapper.apply(val)
                                       .withEach(mVal -> {
                                           synchronized (listenerHolder) {
                                               listenerHolder.set(listenerHolder.get().onValue(mVal));
                                           }
                                       })
                                       .withError(err -> errSummeryCollector.accumulator().accept(errAcc, err))
                                       .anyway(() -> examineCompletion(errSummeryCollector, processes, listenerHolder, errAcc))
                                       .perform())
                .withError(err -> errSummeryCollector.accumulator().accept(errAcc, err))
                .anyway(() -> examineCompletion(errSummeryCollector, processes, listenerHolder, errAcc))
                .perform();
        };
    }

    private <MVal, ErrAcc, ColErr> void examineCompletion(
            Collector<Err, ErrAcc, ErrSummery<ColErr>> errSummeryCollector,
            AtomicInteger processes,
            AtomicReference<StreamListener<MVal, ColErr>> listenerHolder,
            ErrAcc errAcc) {
        if (processes.decrementAndGet() == 0) {
            ErrSummery<ColErr> errSummery = errSummeryCollector.finisher().apply(errAcc);
            if (errSummery.occurred) {
                listenerHolder.get().onError(errSummery.err);
            } else {
                listenerHolder.get().onComplete();
            }
        }
    }

    default StreamActor<Val, Err> filter(Predicate<Val> predicate) {
        return listener -> perform(listener.filter(predicate));
    }

    default <MVal> StreamActor<MVal, Err> mapFilter(Function<Val, Optional<MVal>> mapper) {
        return listener -> perform(listener.fromFilter(mapper));
    }
}
