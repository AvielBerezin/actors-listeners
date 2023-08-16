package actors;

import collectors.Collectors0;
import data.Singleton;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

public final class AsyncStream<Val, Err> {
    private final Consumer<StreamListener<Val, Err>> performer;

    public AsyncStream(Consumer<StreamListener<Val, Err>> performer) {
        this.performer = performer;
    }

    public void perform() {
        performer.accept(StreamListener.nop());
    }

    public void performAwaiting() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.anyway(latch::countDown)
            .perform();
        latch.await();
    }

    public AsyncStream<Val, Err> withEach(Consumer<Val> onValue) {
        return new AsyncStream<>(listener -> performer.accept(listener.withEach(onValue)));
    }

    public AsyncStream<Val, Err> withError(Consumer<Err> onError) {
        return new AsyncStream<>(listener -> performer.accept(listener.withError(onError)));
    }

    public AsyncStream<Val, Err> withCompletion(Runnable onComplete) {
        return new AsyncStream<>(listener -> performer.accept(listener.withCompletion(onComplete)));
    }

    public AsyncStream<Val, Err> anyway(Runnable onDone) {
        return new AsyncStream<>(listener -> performer.accept(listener.anyway(onDone)));
    }

    public static <Val, Err> AsyncStream<Val, Err> of(Collection<Val> collection) {
        return new AsyncStream<>(listener -> {
            for (Val val : collection) {
                listener = listener.onValue(val);
            }
            listener.onComplete();
        });
    }

    public static <Val, Err> AsyncStream<Val, Err> err(Err err) {
        return new AsyncStream<>(listener -> listener.onError(err));
    }


    public <MVal, MErr, ErrAcc> AsyncStream<MVal, MErr> asyncFlatMap(Function<Val, AsyncStream<MVal, Err>> mapper,
                                                                     Collector<Err, ErrAcc, MErr> errCollector) {
        return new AsyncStream<>(listener -> {
            StreamListenerHolder<MVal, MErr> listenerHolder = new StreamListenerHolder<>(listener);
            AtomicReference<Optional<MErr>> mErrHolder = new AtomicReference<>(Optional.empty());
            ErrAcc errAcc = errCollector.supplier().get();
            AtomicInteger tasks = new AtomicInteger(1);
            Object syncer = new Object();
            Runnable onComplete = () -> {
                synchronized (syncer) {
                    if (tasks.decrementAndGet() == 0) {
                        mErrHolder.get().ifPresentOrElse(listenerHolder::notifyError,
                                                         listenerHolder::notifyComplete);
                    }
                }
            };
            Consumer<Err> onError = err -> {
                synchronized (syncer) {
                    errCollector.accumulator().accept(errAcc, err);
                    onComplete.run();
                }
            };
            Consumer<MVal> onEach = mVal -> {
                synchronized (syncer) {
                    listenerHolder.notifyValue(mVal);
                }
            };
            this.map(mapper)
                .withEach(actor -> {
                    synchronized (syncer) {
                        tasks.getAndIncrement();
                        actor.withEach(onEach)
                             .withError(onError)
                             .withCompletion(onComplete)
                             .perform();
                    }
                })
                .withError(onError)
                .withCompletion(onComplete)
                .perform();
        });
    }

    public <MVal, MErr, ErrAcc> AsyncStream<MVal, MErr> asyncFilterMap(Function<Val, AsyncValue<Optional<MVal>, Err>> mapper,
                                                                       Collector<Err, ErrAcc, MErr> errCollector) {
        return this.asyncFlatMap(val -> mapper.apply(val)
                                              .mapStreaming(mVal -> mVal.map(List::of)
                                                                        .<AsyncStream<MVal, Err>>map(AsyncStream::of)
                                                                        .orElse(AsyncStream.of(List.of()))),
                                 errCollector);
    }

    public <MVal, ErrAcc, ColErr> AsyncStream<MVal, ColErr> asyncMap(Function<Val, AsyncValue<MVal, Err>> mapper,
                                                                     Collector<Err, ErrAcc, ColErr> errCollector) {
        return asyncFlatMap(val -> mapper.apply(val)
                                         .map(List::of)
                                         .mapStreaming(AsyncStream::of),
                            errCollector);
    }

    interface ForMain<SubVal, SubErr> {

        default ForMain<SubVal, SubErr> ifVal(Consumer<SubVal> onVal) {
            return this;
        }

        default ForMain<SubVal, SubErr> ifErr(Consumer<SubErr> onErr) {
            return this;
        }

        default ForMain<SubVal, SubErr> ifInProgress(Runnable onInProgress) {
            return this;
        }

        static <SubVal, SubErr> ForMainVal<SubVal, SubErr> val(SubVal val) {
            return new ForMainVal<>(val);
        }

        static <SubVal, SubErr> ForMainErr<SubVal, SubErr> err(SubErr err) {
            return new ForMainErr<>(err);
        }

        static <SubVal, SubErr> ForMainInProgress<SubVal, SubErr> inProgress() {
            return new ForMainInProgress<>();
        }

        final class ForMainVal<SubVal, SubErr> implements ForMain<SubVal, SubErr> {
            private final SubVal val;

            public ForMainVal(SubVal val) {
                this.val = val;
            }

            @Override
            public ForMain<SubVal, SubErr> ifVal(Consumer<SubVal> onVal) {
                onVal.accept(val);
                return this;
            }
        }

        final class ForMainErr<SubVal, SubErr> implements ForMain<SubVal, SubErr> {
            private final SubErr err;

            public ForMainErr(SubErr err) {
                this.err = err;
            }

            @Override
            public ForMain<SubVal, SubErr> ifErr(Consumer<SubErr> onErr) {
                onErr.accept(err);
                return this;
            }
        }

        final class ForMainInProgress<SubVal, SubErr> implements ForMain<SubVal, SubErr> {
            @Override
            public ForMain<SubVal, SubErr> ifInProgress(Runnable onInProgress) {
                onInProgress.run();
                return this;
            }
        }
    }

    interface ForSub<MainVal, MainErr> {
        default ForSub<MainVal, MainErr> ifVals(Consumer<Deque<MainVal>> onVals) {
            return this;
        }

        default ForSub<MainVal, MainErr> ifErr(Consumer<MainErr> onErr) {
            return this;
        }

        default ForSub<MainVal, MainErr> ifComplete(Runnable onComplete) {
            return this;
        }

        static <MainVal, MainErr> ForSubVal<MainVal, MainErr> val(Deque<MainVal> mainVals) {
            return new ForSubVal<>(mainVals);
        }

        static <MainVal, MainErr> ForSubErr<MainVal, MainErr> err(MainErr mainErr) {
            return new ForSubErr<>(mainErr);
        }

        static <MainVal, MainErr> ForSubComplete<MainVal, MainErr> complete() {
            return new ForSubComplete<>();
        }

        final class ForSubVal<MainVal, MainErr> implements ForSub<MainVal, MainErr> {
            private final Deque<MainVal> mainVals;

            public ForSubVal(Deque<MainVal> mainVals) {
                this.mainVals = mainVals;
            }

            @Override
            public ForSubVal<MainVal, MainErr> ifVals(Consumer<Deque<MainVal>> onVals) {
                onVals.accept(mainVals);
                return this;
            }
        }

        final class ForSubErr<MainVal, MainErr> implements ForSub<MainVal, MainErr> {
            private final MainErr mainErr;

            public ForSubErr(MainErr mainErr) {
                this.mainErr = mainErr;
            }

            @Override
            public ForSub<MainVal, MainErr> ifErr(Consumer<MainErr> onErr) {
                onErr.accept(mainErr);
                return this;
            }
        }

        final class ForSubComplete<MainVal, MainErr> implements ForSub<MainVal, MainErr> {
            @Override
            public ForSub<MainVal, MainErr> ifComplete(Runnable onComplete) {
                onComplete.run();
                return this;
            }
        }
    }

    public <MVal, MErr> AsyncStream<MVal, OrdOperationError<Err, MErr>> asyncOrdScan(MVal init,
                                                                                     BiFunction<MVal, Val, AsyncValue<MVal, MErr>> acc) {
        return new AsyncStream<>(listener -> {
            StreamListenerHolder<MVal, OrdOperationError<Err, MErr>> listenerHolder = new StreamListenerHolder<>(listener);
            AtomicReference<ForMain<MVal, MErr>> forMain = new AtomicReference<>(ForMain.val(init));
            AtomicReference<ForSub<Val, Err>> forSub = new AtomicReference<>(ForSub.val(new LinkedList<>()));
            Object syncer = new Object();
            listenerHolder.notifyValue(init);
            this.withEach(val -> {
                    synchronized (syncer) {
                        if (listenerHolder.isTerminated()) {
                            return;
                        }
                        forMain.get()
                               .ifVal(mVal -> accChain(listenerHolder, mVal, acc, val, forSub, forMain, syncer))
                               .ifInProgress(() -> forSub.get().ifVals(vals -> vals.addLast(val)));
                    }
                })
                .withError(err -> {
                    synchronized (syncer) {
                        forMain.get()
                               .ifVal(mVal -> listenerHolder.notifyError(OrdOperationError.main(err)))
                               .ifErr(mErr -> listenerHolder.notifyError(OrdOperationError.both(err, mErr)))
                               .ifInProgress(() -> forSub.set(ForSub.err(err)));
                    }
                })
                .withCompletion(() -> {
                    synchronized (syncer) {
                        forMain.get()
                               .ifVal(mVal -> listenerHolder.notifyComplete())
                               .ifErr(mErr -> listenerHolder.notifyError(OrdOperationError.sub(mErr)))
                               .ifInProgress(() -> forSub.set(ForSub.complete()));
                    }
                })
                .perform();
        });
    }

    private <MErr, MVal> void accChain(StreamListenerHolder<MVal, OrdOperationError<Err, MErr>> listenerHolder,
                                       MVal init,
                                       BiFunction<MVal, Val, AsyncValue<MVal, MErr>> acc,
                                       Val val,
                                       AtomicReference<ForSub<Val, Err>> forSub,
                                       AtomicReference<ForMain<MVal, MErr>> forMain,
                                       Object syncer) {
        forMain.set(ForMain.inProgress());
        acc.apply(init, val)
           .withValue(mVal -> {
               synchronized (syncer) {
                   listenerHolder.notifyValue(mVal);
                   forSub.get()
                         .ifErr(err -> listenerHolder.notifyError(OrdOperationError.main(err)))
                         .ifComplete(listenerHolder::notifyComplete)
                         .ifVals(vals -> {
                             if (vals.isEmpty()) {
                                 forMain.set(ForMain.val(mVal));
                             } else {
                                 accChain(listenerHolder, init, acc, val, forSub, forMain, syncer);
                             }
                         });
               }
           })
           .withError(mErr -> {
               synchronized (syncer) {
                   forSub.get()
                         .ifVals(ignored -> forMain.set(ForMain.err(mErr)))
                         .ifErr(err -> listenerHolder.notifyError(OrdOperationError.both(err, mErr)))
                         .ifComplete(() -> listenerHolder.notifyError(OrdOperationError.sub(mErr)));
               }
           })
           .perform();
    }

    public <MVal, MErr> AsyncStream<MVal, OrdOperationError<Err, MErr>> asyncOrdMap(Function<Val, AsyncValue<MVal, MErr>> mapper) {
        return this.<Optional<MVal>, MErr>asyncOrdScan(Optional.empty(), (opt, val) -> mapper.apply(val).map(Optional::of))
                   .filterMap(Function.identity());
    }

    public <MVal, MErr> AsyncStream<MVal, OrdOperationError<Err, MErr>> asyncOrdFlatMap(Function<Val, AsyncStream<MVal, MErr>> mapper) {
        return new AsyncStream<>(listener -> {
            StreamListenerHolder<MVal, OrdOperationError<Err, MErr>> listenerHolder = new StreamListenerHolder<>(listener);
            this.asyncOrdMap(val -> mapper.apply(val)
                                          .withEach(listenerHolder::notifyValue)
                                          .collect(Collectors0.ignoring()))
                .withError(listenerHolder::notifyError)
                .withCompletion(listenerHolder::notifyComplete)
                .perform();
        });
    }

    public <Res, MErr> AsyncValue<Res, OrdOperationError<Err, MErr>> asyncOrdCollect(Res init,
                                                                                     BiFunction<Res, Val, AsyncValue<Res, MErr>> acc) {
        return this.asyncOrdScan(init, acc)
                   .collect(Collectors0.last())
                   .map(Optional::get);
    }

    public <MVal> AsyncStream<MVal, Err> scan(MVal init,
                                              BiFunction<MVal, Val, MVal> acc) {
        return new AsyncStream<>(listener -> performer.accept(listener.onValue(init).fromAcc(init, acc)));
    }

    public <MVal> AsyncStream<MVal, Err> map(Function<Val, MVal> mapper) {
        return new AsyncStream<>(listener -> performer.accept(listener.from(mapper)));
    }

    public AsyncStream<Val, Err> filter(Predicate<Val> predicate) {
        return new AsyncStream<>(listener -> performer.accept(listener.fromFilter(predicate)));
    }

    public <MVal> AsyncStream<MVal, Err> filterMap(Function<Val, Optional<MVal>> mapper) {
        return new AsyncStream<>(listener -> performer.accept(listener.fromFilterMap(mapper)));
    }

    public <ColVal, Acc> AsyncValue<ColVal, Err> collect(Collector<Val, Acc, ColVal> collector) {
        return new AsyncValue<>(listener -> {
            Acc acc = collector.supplier().get();
            this.withEach(val -> collector.accumulator().accept(acc, val))
                .withError(listener::onError)
                .withCompletion(() -> listener.onValue(collector.finisher().apply(acc)))
                .perform();
        });
    }

    public <MErr> AsyncStream<Val, MErr> mapError(Function<Err, MErr> mapper) {
        return new AsyncStream<>(listener -> this.performer.accept(listener.fromError(mapper)));
    }

    public <MErr> AsyncStream<Val, MErr> catchError(Function<Err, AsyncValue<Singleton, MErr>> catcher) {
        return new AsyncStream<>(listener -> this.performer.accept(listener.fromCatcher(catcher)));
    }
}
