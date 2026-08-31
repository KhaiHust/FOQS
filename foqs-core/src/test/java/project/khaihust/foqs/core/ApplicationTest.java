package project.khaihust.foqs.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class ApplicationTest {

    @Test
    @DisplayName("Should run main method without throwing exceptions")
    void testMainMethod() {
        assertThatCode(() -> Application.main(new String[]{}))
                .doesNotThrowAnyException();
    }
}
