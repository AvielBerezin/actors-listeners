package actors;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public final class AsyncValue<Val, Err> {
    private final Consumer<ValueListener<Val, Err>> performer;

    public AsyncValue(Consumer<ValueListener<Val, Err>> performer) {this.performer = performer;}

    public void perform() {
        performer.accept(ValueListener.nop());
    }

    public void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    public <Exc extends Throwable> Val await(Function<Err, Exc> exceptor) throws InterruptedException, Exc {
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

    public static <Val, Err> AsyncValue<Val, Err> of(Val val) {
        return new AsyncValue<>(listener -> listener.onValue(val));
    }

    public static <Val, Err> AsyncValue<Val, Err> err(Err err) {
        return new AsyncValue<>(listener -> listener.onError(err));
    }

    public AsyncValue<Err, Val> flip() {
        return new AsyncValue<>(listener -> performer.accept(listener.flip()));
    }

    public AsyncValue<Val, Err> withValue(Consumer<Val> onValue) {
        return new AsyncValue<>(listener -> performer.accept(listener.withValue(onValue)));
    }

    public AsyncValue<Val, Err> withError(Consumer<Err> onError) {
        return flip().withValue(onError).flip();
    }

    public AsyncValue<Val, Err> anyway(Runnable onDone) {
        return new AsyncValue<>(listener -> performer.accept(listener.anyway(onDone)));
    }

    public <MVal> AsyncValue<MVal, Err> flatMap(Function<Val, AsyncValue<MVal, Err>> mapper) {
        return new AsyncValue<>(listener -> performer.accept(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                AsyncValue<MVal, Err> mValErrAsyncValue = mapper.apply(val);
                mValErrAsyncValue.performer.accept(listener);
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        }));
    }

    public <MVal> AsyncValue<MVal, Err> map(Function<Val, MVal> mapper) {
        return new AsyncValue<>(listener -> performer.accept(new ValueListener<>() {
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

    public <MErr> AsyncValue<Val, MErr> flatMapError(Function<Err, AsyncValue<Val, MErr>> mapper) {
        return flip().flatMap(mapper.andThen(AsyncValue::flip)).flip();
    }

    public <MErr> AsyncValue<Val, MErr> mapError(Function<Err, MErr> mapper) {
        return flip().map(mapper).flip();
    }

    public <MVal> AsyncStream<MVal, Err> mapStreaming(Function<Val, AsyncStream<MVal, Err>> mapper) {
        return new AsyncStream<>(listener -> {
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
