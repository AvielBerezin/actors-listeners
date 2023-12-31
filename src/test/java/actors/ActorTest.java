package actors;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ActorTest {

    <Value> AsyncStream<Value, RuntimeException> asyncActor(
            ScheduledExecutorService scheduler,
            Collection<Value> collection) {
        return new AsyncStream<>(listener -> applySchedulerToListener(scheduler,
                                                                      new AtomicReference<>(listener),
                                                                      collection.iterator()));
    }

    private static <Value> void applySchedulerToListener(
            ScheduledExecutorService scheduler,
            AtomicReference<StreamListener<Value, RuntimeException>> listenerHolder,
            Iterator<Value> iterator) {
        Runnable onNext = () -> {
            listenerHolder.set(listenerHolder.get().onValue(iterator.next()));
            applySchedulerToListener(scheduler, listenerHolder, iterator);
        };
        Runnable onDone = () -> listenerHolder.get().onComplete();
        scheduler.schedule(iterator.hasNext() ? onNext : onDone, 500L, TimeUnit.MILLISECONDS);
    }

    static class MultiExcept extends Exception {
        private final List<? extends Exception> exceptions;

        MultiExcept(List<? extends Exception> exceptions) {this.exceptions = exceptions;}

        @Override
        public String getMessage() {
            return exceptions.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"));
        }
    }

    @Test
    void aTest() throws InterruptedException {
        asyncActor(Executors.newSingleThreadScheduledExecutor(), List.of(2, 4, 5, 6, 3))
                .withEach(x -> System.out.println("inspecting :" + x))
                .asyncFlatMap(x -> {
                             if (x % 2 == 0) {
                                 return asyncActor(Executors.newSingleThreadScheduledExecutor(),
                                                   Stream.iterate(0, y -> y + 1)
                                                         .limit(x)
                                                         .toList())
                                         .map(y -> "async %d %d".formatted(x, y));
                             } else {
                                 return AsyncStream.<Integer, RuntimeException>of(Stream.iterate(0, y -> y + 1)
                                                                                        .limit(x)
                                                                                        .toList())
                                                   .map(y -> "sync %d %d".formatted(x, y));
                             }
                         },
                              Collectors.collectingAndThen(Collectors.toList(),
                                                      MultiExcept::new))
                .withEach(x -> System.out.println("flattened value: " + x))
                .withCompletion(() -> System.out.println("DONE Stream"))
                .collect(Collectors.toList())
                .withValue(val -> System.out.println("on value: " + val))
                .withError(err -> {
                    System.out.println("on error");
                    err.printStackTrace();
                })
                .performAwaiting();
    }

    @Test
    void filterTest() throws InterruptedException {
        asyncActor(Executors.newSingleThreadScheduledExecutor(), List.of(1, 2, 3, 4, 5))
                .withEach(x -> System.out.printf("%d%n", x))
                .filter(x -> x % 2 == 0)
                .withEach(x -> System.out.printf("filtered %d%n", x))
                .performAwaiting();
    }

    @Test
    void mapFilter() throws InterruptedException {
        asyncActor(Executors.newSingleThreadScheduledExecutor(),
                   List.of("a", "Bbbb", "123", "dasd", "7", "12", "not-a-0number"))
                .withEach(x -> System.out.println("checking: " + x))
                .filterMap(x -> {
                    try {
                        return Optional.of(Integer.parseInt(x));
                    } catch (NumberFormatException e) {
                        return Optional.empty();
                    }
                })
                .withEach(x -> System.out.println("found a number: " + x))
                .withCompletion(() -> System.out.println("DONE"))
                .collect(Collectors.toList())
                .withValue(xs -> System.out.println("finally got " + xs))
                .performAwaiting();
    }

    static class MyException extends Exception {
        public MyException() {
        }

        public MyException(String message) {
            super(message);
        }

        public MyException(String message, Throwable cause) {
            super(message, cause);
        }

        public MyException(Throwable cause) {
            super(cause);
        }

        public MyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    @Test
    void performAwaiting() throws InterruptedException, MyException {
        List<Integer> result = AsyncValue.<Integer, String>of(1)
                                         .withValue(val -> System.out.println("starting with " + val))
                                         .map(val -> List.of(1, val, 2, val, 3))
                                         .withValue(vals -> System.out.println("mapped to " + vals))
                                         .withError(msg -> System.err.println("before error insertion error: " + msg))
                                         .flatMap(ints -> AsyncValue.<List<Integer>, String>err("ohh nooo"))
                                         .withValue(val -> System.out.println("inside perform " + val))
                                         .withValue(val -> System.out.println("ending with this message"))
                                         .withError(msg -> System.err.println("there is an error: " + msg))
                                         .await(MyException::new);
        System.out.println("result = " + result);
    }
}