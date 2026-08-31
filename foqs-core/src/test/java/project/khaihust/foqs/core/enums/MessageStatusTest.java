package project.khaihust.foqs.core.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageStatusTest {

    @ParameterizedTest
    @CsvSource({
            "0, READY",
            "1, LEASED",
            "2, COMPLETED",
            "3, DEAD_LETTER"
    })
    @DisplayName("Should return correct enum from status code")
    void testFromCode_ValidCodes(int code, MessageStatus expectedStatus) {
        MessageStatus status = MessageStatus.fromCode(code);
        assertThat(status).isEqualTo(expectedStatus);
        assertThat(status.getCode()).isEqualTo(code);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for unknown status code")
    void testFromCode_InvalidCode() {
        assertThatThrownBy(() -> MessageStatus.fromCode(99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown status code: 99");

        assertThatThrownBy(() -> MessageStatus.fromCode(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown status code: -1");
    }
}
