package actors;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ValueActor<Val, Err> {
    void perform(ValueListener<Val, Err> listener);

    default void perform() {
        perform(ValueListener.nop());
    }

    default void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    static <Val, Err> ValueActor<Val, Err> of(Val val) {
        return listener -> listener.onValue(val);
    }

    static <Val, Err> ValueActor<Val, Err> err(Err err) {
        return listener -> listener.onError(err);
    }

    default ValueActor<Err, Val> flip() {
        return listener -> perform(listener.flip());
    }

    default ValueActor<Val, Err> withValue(Consumer<Val> onValue) {
        return listener -> perform(listener.withValue(onValue));
    }

    default ValueActor<Val, Err> withError(Consumer<Err> onError) {
        return flip().withValue(onError).flip();
    }

    default ValueActor<Val, Err> anyway(Runnable onDone) {
        return listener -> perform(listener.anyway(onDone));
    }


    default <MVal> ValueActor<MVal, Err> map(Function<Val, MVal> mapper) {
        return listener -> perform(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                listener.onValue(mapper.apply(val));
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        });
    }

    default <MVal> ValueActor<MVal, Err> flatMap(Function<Val, ValueActor<MVal, Err>> mapper) {
        return listener -> perform(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                mapper.apply(val).perform(listener);
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        });
    }

    default <MErr> ValueActor<Val, MErr> mapError(Function<Err, MErr> mapper) {
        return flip().map(mapper).flip();
    }

    default <MErr> ValueActor<Val, MErr> flatMapError(Function<Err, ValueActor<Val, MErr>> mapper) {
        return flip().flatMap(mapper.andThen(ValueActor::flip)).flip();
    }

    default <MVal> StreamActor<MVal, Err> mapStreaming(Function<Val, StreamActor<MVal, Err>> mapper) {
        return listener -> this.perform(new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                mapper.apply(val).perform(listener);
            }

            @Override
            public void onError(Err err) {
                listener.onError(err);
            }
        });
    }
}
