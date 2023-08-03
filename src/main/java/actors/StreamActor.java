package actors;

import java.util.Collection;
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

public final class StreamActor<Val, Err> {
    private final Consumer<StreamListener<Val, Err>> performer;

    public StreamActor(Consumer<StreamListener<Val, Err>> performer) {this.performer = performer;}

    public void perform() {
        performer.accept(StreamListener.nop());
    }

    public void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    public StreamActor<Val, Err> withEach(Consumer<Val> onValue) {
        return new StreamActor<>(listener -> performer.accept(listener.withEach(onValue)));
    }

    public StreamActor<Val, Err> withError(Consumer<Err> onError) {
        return new StreamActor<>(listener -> performer.accept(listener.withError(onError)));
    }

    public StreamActor<Val, Err> withCompletion(Runnable onComplete) {
        return new StreamActor<>(listener -> performer.accept(listener.withCompletion(onComplete)));
    }

    public StreamActor<Val, Err> anyway(Runnable onDone) {
        return new StreamActor<>(listener -> performer.accept(listener.anyway(onDone)));
    }

    public static <Val, Err> StreamActor<Val, Err> of(Collection<Val> collection) {
        return new StreamActor<>(listener -> {
            for (Val val : collection) {
                listener = listener.onValue(val);
            }
            listener.onComplete();
        });
    }

    public static <Val, Err> StreamActor<Val, Err> err(Err err) {
        return new StreamActor<>(listener -> listener.onError(err));
    }

    public <ColVal, Acc> ValueActor<ColVal, Err> collect(Collector<Val, Acc, ColVal> collector) {
        return new ValueActor<>(listener -> {
            Acc acc = collector.supplier().get();
            this.withEach(val -> collector.accumulator().accept(acc, val))
                .withError(listener::onError)
                .withCompletion(() -> listener.onValue(collector.finisher().apply(acc)))
                .perform();
        });
    }

    public <MVal> StreamActor<MVal, Err> map(Function<Val, MVal> mapper) {
        return new StreamActor<>(listener -> performer.accept(listener.from(mapper)));
    }

    public record ErrSummery<Err>(boolean occurred, Err err) {}

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap(
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

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap0(
            Function<Val, StreamActor<MVal, Err>> mapper,
            Collector<Err, ErrAcc, ErrSummery<ColErr>> errSummeryCollector) {
        return new StreamActor<>(listener -> {
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
        });
    }

    public <MVal, ErrAcc, ColErr> void examineCompletion(
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

    public StreamActor<Val, Err> filter(Predicate<Val> predicate) {
        return new StreamActor<>(listener -> performer.accept(listener.filter(predicate)));
    }

    public <MVal> StreamActor<MVal, Err> mapFilter(Function<Val, Optional<MVal>> mapper) {
        return new StreamActor<>(listener -> performer.accept(listener.fromFilter(mapper)));
    }
}
