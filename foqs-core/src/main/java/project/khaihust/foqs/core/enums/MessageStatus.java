package project.khaihust.foqs.core.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@Getter
@RequiredArgsConstructor
public enum MessageStatus {
    READY(0),
    LEASED(1),
    COMPLETED(2),
    DEAD_LETTER(3);

    private final int code;

    public static MessageStatus fromCode(int code) {
        return Arrays.stream(values())
                .filter(status -> status.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown status code: " + code));
    }
}
