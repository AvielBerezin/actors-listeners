package actors;

import collectors.Collectors0;

import java.util.Collection;
import java.util.LinkedList;
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

    public StreamActor(Consumer<StreamListener<Val, Err>> performer) {
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

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> mapVal(Function<Val, ValueActor<MVal, Err>> mapper,
                                                                   Collector<Err, ErrAcc, ColErr> errCollector) {
        return mapVal0(mapper,
                       Collectors.teeing(Collectors0.exists(),
                                         errCollector,
                                         ErrSummery::new));
    }

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> mapVal0(Function<Val, ValueActor<MVal, Err>> mapper,
                                                                    Collector<Err, ErrAcc, ErrSummery<ColErr>> errSummeryCollector) {
        return new StreamActor<>(listener -> {
            AtomicInteger processes = new AtomicInteger(1);
            AtomicReference<StreamListener<MVal, ColErr>> listenerHolder = new AtomicReference<>(listener);
            ErrAcc errAcc = errSummeryCollector.supplier().get();
            this.withEach(val -> processes.incrementAndGet())
                .withEach(val -> mapper.apply(val)
                                       .withValue(mVal -> {
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

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap(Function<Val, StreamActor<MVal, Err>> mapper,
                                                                    Collector<Err, ErrAcc, ColErr> errCollector) {
        return flatMap0(mapper,
                        Collectors.teeing(Collectors0.exists(),
                                          errCollector,
                                          ErrSummery::new));
    }

    public <MVal, ErrAcc, ColErr> StreamActor<MVal, ColErr> flatMap0(Function<Val, StreamActor<MVal, Err>> mapper,
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

    public <MVal> StreamActor<MVal, Err> mapValOrd(Function<Val, ValueActor<MVal, Err>> mapper) {
        return new StreamActor<>(listener -> {
            AtomicReference<Optional<StreamListener<MVal, Err>>> listenerHolder = new AtomicReference<>(Optional.of(listener));
            LinkedList<ValueActor<MVal, Err>> actors = new LinkedList<>();
            AtomicBoolean mainDone = new AtomicBoolean(false);
            Object syncer = new Object();
            this.map(mapper)
                .map(mValAct -> mValAct.withValue(mVal -> {
                                           synchronized (syncer) {
                                               listenerHolder.get()
                                                             .map(lis -> lis.onValue(mVal))
                                                             .map(Optional::of)
                                                             .ifPresent(listenerHolder::set);
                                           }
                                       })
                                       .withError(err -> {
                                           synchronized (syncer) {
                                               listenerHolder.get()
                                                             .ifPresent(lis -> {
                                                                 lis.onError(err);
                                                                 listenerHolder.set(Optional.empty());
                                                                 actors.clear();
                                                             });
                                           }
                                       })
                                       .withValue(mVal -> {
                                           synchronized (syncer) {
                                               actors.removeFirst();
                                               if (listenerHolder.get().isEmpty()) {
                                                   actors.clear();
                                               } else if (!actors.isEmpty()) {
                                                   actors.peekFirst().perform();
                                               } else if (mainDone.get()) {
                                                   listenerHolder.get().get().onComplete();
                                                   listenerHolder.set(Optional.empty());
                                               }
                                           }
                                       }))
                .withEach(mValAct -> {
                    synchronized (syncer) {
                        if (listenerHolder.get().isEmpty()) {
                            actors.clear();
                        } else {
                            actors.addLast(mValAct);
                            if (actors.size() == 1) {
                                actors.peekFirst().perform();
                            }
                        }
                    }
                })
                .withError(err -> {
                    synchronized (syncer) {
                        listenerHolder.get()
                                      .ifPresent(lis -> {
                                          lis.onError(err);
                                          listenerHolder.set(Optional.empty());
                                          actors.clear();
                                      });
                    }
                })
                .withCompletion(() -> {
                    synchronized (syncer) {
                        if (actors.isEmpty()) {
                            listenerHolder.get()
                                          .ifPresent(lis -> {
                                              lis.onComplete();
                                              listenerHolder.set(Optional.empty());
                                          });
                        }
                        mainDone.set(true);
                    }
                })
                .perform();
        });
    }

    public <MVal> StreamActor<MVal, Err> flatMapOrd(Function<Val, StreamActor<MVal, Err>> mapper) {
        return new StreamActor<>(listener -> {
            AtomicReference<Optional<StreamListener<MVal, Err>>> listenerHolder = new AtomicReference<>(Optional.of(listener));
            this.mapValOrd(val -> mapper.apply(val)
                                        .withEach(mVal -> {
                                            listenerHolder.get().ifPresent(lis -> {
                                                listenerHolder.set(Optional.of(lis.onValue(mVal)));
                                            });
                                        })
                                        .withError(err -> {
                                            listenerHolder.get().ifPresent(lis -> {
                                                lis.onError(err);
                                                listenerHolder.set(Optional.empty());
                                            });
                                        })
                                        .withCompletion(() -> {
                                            listenerHolder.get().ifPresent(lis -> {
                                                lis.onComplete();
                                                listenerHolder.set(Optional.empty());
                                            });
                                        })
                                        .collect(Collectors0.ignoring()))
                .perform();
        });
    }

    public StreamActor<Val, Err> filter(Predicate<Val> predicate) {
        return new StreamActor<>(listener -> performer.accept(listener.fromFilter(predicate)));
    }

    public <MVal> StreamActor<MVal, Err> mapFilter(Function<Val, Optional<MVal>> mapper) {
        return new StreamActor<>(listener -> performer.accept(listener.fromMapFilter(mapper)));
    }
}
