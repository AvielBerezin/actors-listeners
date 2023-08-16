package actors;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncStreamTest {
    @Test
    void testMapValOrd() {
        assertThatThrownBy(() -> AsyncStream.<Integer, String>of(List.of(1, 2, 3, 4))
                                            .withEach(i -> System.out.println("inspecting " + i))
                                            .asyncOrdMap(i -> i != 3
                                                            ? AsyncValue.of((char) ('a' + i))
                                                            : AsyncValue.err("oh no"))
                                            .withEach(System.out::println)
                                            .withError(System.err::println)
                                            .collect(Collectors.toList())
                                            .withValue(vals -> System.out.println("received result " + vals))
                                            .withError(err -> System.err.println("received error " + err))
                                            .await(Exception::new))
                .hasMessage("oh no");
    }
}