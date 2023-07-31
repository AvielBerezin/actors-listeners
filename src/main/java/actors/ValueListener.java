package actors;


import java.util.function.Consumer;

public interface ValueListener<Val, Err> {
    void onValue(Val val);

    void onError(Err err);

    static <Val, Err> ValueListener<Val, Err> nop() {
        return new ValueListener<>() {
            @Override
            public void onValue(Val val) {}

            @Override
            public void onError(Err err) {}
        };
    }

    default ValueListener<Err, Val> flip() {
        return new ValueListener<>() {
            @Override
            public void onValue(Err err) {
                ValueListener.this.onError(err);
            }

            @Override
            public void onError(Val val) {
                ValueListener.this.onValue(val);
            }
        };
    }

    default ValueListener<Val, Err> withValue(Consumer<Val> onValue) {
        return new ValueListener<>() {
            @Override
            public void onValue(Val val) {
                onValue.accept(val);
                ValueListener.this.onValue(val);
            }

            @Override
            public void onError(Err err) {
                ValueListener.this.onError(err);
            }
        };
    }

    default ValueListener<Val, Err> withError(Consumer<Err> onError) {
        return flip().withValue(onError).flip();
    }

    default ValueListener<Val, Err> anyway(Runnable onDone) {
        return this.withValue(val -> onDone.run())
                   .withError(err -> onDone.run());
    }
}
