package actors;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class StreamListenerHolder<Val, Err> {
    final private AtomicReference<Optional<StreamListener<Val, Err>>> listener;

    public StreamListenerHolder(StreamListener<Val, Err> listener) {
        this.listener = new AtomicReference<>(Optional.of(listener));
    }

    public void notifyValue(Val val) {
        synchronized (listener) {
            listener.get().ifPresent(lis -> {
                StreamListener<Val, Err> nextListener = lis.onValue(val);
                listener.set(Optional.of(nextListener));
            });
        }
    }

    public void notifyError(Err err) {
        synchronized (listener) {
            listener.get().ifPresent(lis -> {
                lis.onError(err);
                listener.set(Optional.empty());
            });
        }
    }

    public void notifyComplete() {
        synchronized (listener) {
            listener.get().ifPresent(lis -> {
                lis.onComplete();
                listener.set(Optional.empty());
            });
        }
    }

    public boolean isTerminated() {
        synchronized (listener) {
            return listener.get().isEmpty();
        }
    }
}
