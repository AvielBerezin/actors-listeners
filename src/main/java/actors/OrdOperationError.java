package actors;

import java.util.function.BiFunction;
import java.util.function.Function;

interface OrdOperationError<MainErr, SubErr> {
    <ResultErr> ResultErr accept(Function<MainErr, ResultErr> onMainErr,
                                 Function<SubErr, ResultErr> onSubErr,
                                 BiFunction<MainErr, SubErr, ResultErr> onBothErr);

    static <MainErr, SubErr> OrdOperationErrorMain<MainErr, SubErr> main(MainErr mainErr) {
        return new OrdOperationErrorMain<>(mainErr);
    }

    static <MainErr, SubErr> OrdOperationErrorSub<MainErr, SubErr> sub(SubErr subErr) {
        return new OrdOperationErrorSub<>(subErr);
    }

    static <MainErr, SubErr> OrdOperationErrorBoth<MainErr, SubErr> both(MainErr mainErr, SubErr subErr) {
        return new OrdOperationErrorBoth<>(mainErr, subErr);
    }

    final class OrdOperationErrorMain<MainErr, SubErr> implements OrdOperationError<MainErr, SubErr> {
        private final MainErr mainErr;

        public OrdOperationErrorMain(MainErr mainErr) {
            this.mainErr = mainErr;
        }

        @Override
        public <ResultErr> ResultErr accept(Function<MainErr, ResultErr> onMainErr,
                                            Function<SubErr, ResultErr> onSubErr,
                                            BiFunction<MainErr, SubErr, ResultErr> onBothErr) {
            return onMainErr.apply(mainErr);
        }
    }

    final class OrdOperationErrorSub<MainErr, SubErr> implements OrdOperationError<MainErr, SubErr> {
        private final SubErr subErr;

        public OrdOperationErrorSub(SubErr subErr) {
            this.subErr = subErr;
        }

        @Override
        public <ResultErr> ResultErr accept(Function<MainErr, ResultErr> onMainErr,
                                            Function<SubErr, ResultErr> onSubErr,
                                            BiFunction<MainErr, SubErr, ResultErr> onBothErr) {
            return onSubErr.apply(subErr);
        }
    }

    final class OrdOperationErrorBoth<MainErr, SubErr> implements OrdOperationError<MainErr, SubErr> {
        private final MainErr mainErr;
        private final SubErr subErr;

        public OrdOperationErrorBoth(MainErr mainErr, SubErr subErr) {
            this.mainErr = mainErr;
            this.subErr = subErr;
        }

        @Override
        public <ResultErr> ResultErr accept(Function<MainErr, ResultErr> onMainErr,
                                            Function<SubErr, ResultErr> onSubErr,
                                            BiFunction<MainErr, SubErr, ResultErr> onBothErr) {
            return onBothErr.apply(mainErr, subErr);
        }
    }
}
