package actors;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public final class ValueActor<Val, Err> {
    private final Consumer<ValueListener<Val, Err>> performer;

    public ValueActor(Consumer<ValueListener<Val, Err>> performer) {this.performer = performer;}

    public void perform() {
        performer.accept(ValueListener.nop());
    }

    public void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    public <Exc extends Throwable> Val performAwaiting(Function<Err, Exc> exceptor) throws InterruptedException, Exc {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Optional<Val>> result = new AtomicReference<>(Optional.empty());
        AtomicReference<Optional<Err>> error = new AtomicReference<>(Optional.empty());
        this.map(Optional::of)
            .withValue(result::set)
            .mapError(Optional::of)
            .withError(error::set)
            .anyway(latch::countDown)
            .perform();
        latch.await();
        if (error.get().isPresent()) {
            throw exceptor.apply(error.get().get());
        } else if (result.get().isPresent()) {
            return result.get().get();
        } else {
            throw new RuntimeException("algorithmic error, this exception should not be possible");
        }
    }

    public static <Val, Err> ValueActor<Val, Err> of(Val val) {
        return new ValueActor<>(listener -> listener.onValue(val));
    }

    public static <Val, Err> ValueActor<Val, Err> err(Err err) {
        return new ValueActor<>(listener -> listener.onError(err));
    }

    public ValueActor<Err, Val> flip() {
        return new ValueActor<>(listener -> performer.accept(listener.flip()));
    }

    public ValueActor<Val, Err> withValue(Consumer<Val> onValue) {
        return new ValueActor<>(listener -> performer.accept(listener.withValue(onValue)));
    }

    public ValueActor<Val, Err> withError(Consumer<Err> onError) {
        return flip().withValue(onError).flip();
    }

    public ValueActor<Val, Err> anyway(Runnable onDone) {
        return new ValueActor<>(listener -> performer.accept(listener.anyway(onDone)));
    }


    public <MVal> ValueActor<MVal, Err> map(Function<Val, MVal> mapper) {
        return new ValueActor<>(listener -> performer.accept(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                listener.onValue(mapper.apply(val));
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        }));
    }

    public <MVal> ValueActor<MVal, Err> flatMap(Function<Val, ValueActor<MVal, Err>> mapper) {
        return new ValueActor<>(listener -> performer.accept(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                ValueActor<MVal, Err> mValErrValueActor = mapper.apply(val);
                mValErrValueActor.performer.accept(listener);
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        }));
    }

    public <MErr> ValueActor<Val, MErr> mapError(Function<Err, MErr> mapper) {
        return flip().map(mapper).flip();
    }

    public <MErr> ValueActor<Val, MErr> flatMapError(Function<Err, ValueActor<Val, MErr>> mapper) {
        return flip().flatMap(mapper.andThen(ValueActor::flip)).flip();
    }

    public <MVal> StreamActor<MVal, Err> mapStreaming(Function<Val, StreamActor<MVal, Err>> mapper) {
        return new StreamActor<>(listener -> {
            AtomicReference<StreamListener<MVal, Err>> listenerHolder = new AtomicReference<>(listener);
            performer.accept(new ValueListener<>() {
                @Override
                public void onValue(Val val) {
                    mapper.apply(val)
                          .withEach(mVal -> listenerHolder.set(listenerHolder.get().onValue(mVal)))
                          .withError(err -> listenerHolder.get().onError(err))
                          .withCompletion(() -> listenerHolder.get().onComplete())
                          .perform();
                }

                @Override
                public void onError(Err err) {
                    listener.onError(err);
                }
            });
        });
    }
}
