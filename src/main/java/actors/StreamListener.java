package actors;

import data.Singleton;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface StreamListener<Val, Err> {
    StreamListener<Val, Err> onValue(Val val);

    void onError(Err err);

    void onComplete();

    default StreamListener<Val, Err> withEach(Consumer<Val> onValue) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, Err> onValue(Val val) {
                onValue.accept(val);
                return StreamListener.this.onValue(val).withEach(onValue);
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default StreamListener<Val, Err> withError(Consumer<Err> onError) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, Err> onValue(Val val) {
                return StreamListener.this.onValue(val)
                                          .withError(onError);
            }

            @Override
            public void onError(Err err) {
                onError.accept(err);
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default StreamListener<Val, Err> withCompletion(Runnable onComplete) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, Err> onValue(Val val) {
                return StreamListener.this.onValue(val)
                                          .withCompletion(onComplete);
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                onComplete.run();
                StreamListener.this.onComplete();
            }
        };
    }

    default StreamListener<Val, Err> anyway(Runnable onDone) {
        return this.withCompletion(onDone)
                   .withError(err -> onDone.run());

    }

    default <FVal> StreamListener<FVal, Err> from(Function<FVal, Val> mapper) {
        return new StreamListener<>() {
            @Override
            public StreamListener<FVal, Err> onValue(FVal val) {
                return StreamListener.this.onValue(mapper.apply(val)).from(mapper);
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default StreamListener<Val, Err> fromFilter(Predicate<Val> predicate) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, Err> onValue(Val val) {
                if (predicate.test(val)) {
                    return StreamListener.this.onValue(val)
                                              .fromFilter(predicate);
                } else {
                    return StreamListener.this.fromFilter(predicate);
                }
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    static <Val, Err> StreamListener<Val, Err> nop() {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, Err> onValue(Val val) {return nop();}

            @Override
            public void onError(Err err) {}

            @Override
            public void onComplete() {}
        };
    }

    default <FVal> StreamListener<FVal, Err> fromFilterMap(Function<FVal, Optional<Val>> mapper) {
        return new StreamListener<>() {
            @Override
            public StreamListener<FVal, Err> onValue(FVal fVal) {
                return mapper.apply(fVal)
                             .map(StreamListener.this::onValue)
                             .orElse(StreamListener.this)
                             .fromFilterMap(mapper);
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default <FErr> StreamListener<Val, FErr> fromError(Function<FErr, Err> mapper) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, FErr> onValue(Val val) {
                return StreamListener.this.onValue(val).fromError(mapper);
            }

            @Override
            public void onError(FErr fErr) {
                StreamListener.this.onError(mapper.apply(fErr));
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default <FErr> StreamListener<Val, FErr> fromCatcher(Function<FErr, AsyncValue<Singleton, Err>> catcher) {
        return new StreamListener<>() {
            @Override
            public StreamListener<Val, FErr> onValue(Val val) {
                return StreamListener.this.onValue(val).fromCatcher(catcher);
            }

            @Override
            public void onError(FErr fErr) {
                catcher.apply(fErr)
                       .withValue(ignoring -> StreamListener.this.onComplete())
                       .withError(StreamListener.this::onError)
                       .perform();
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }

    default <FVal> StreamListener<FVal, Err> fromAcc(Val init, BiFunction<Val, FVal, Val> acc) {
        return new StreamListener<>() {
            @Override
            public StreamListener<FVal, Err> onValue(FVal fVal) {
                Val next = acc.apply(init, fVal);
                return StreamListener.this.onValue(next).fromAcc(next, acc);
            }

            @Override
            public void onError(Err err) {
                StreamListener.this.onError(err);
            }

            @Override
            public void onComplete() {
                StreamListener.this.onComplete();
            }
        };
    }
}
