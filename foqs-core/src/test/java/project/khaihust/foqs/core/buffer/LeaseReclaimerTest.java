package project.khaihust.foqs.core.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.impl.LeaseReclaimer;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaseReclaimerTest {

    @Mock
    private ISingleShardQueueRepository queueRepository;

    private LeaseReclaimer leaseReclaimer;

    @AfterEach
    void tearDown() throws Exception {
        if (leaseReclaimer != null) {
            leaseReclaimer.close();
        }
    }

    @Test
    @DisplayName("Should invoke reclaimExpiredLeases on repository when reclaimLease is called")
    void testReclaimLease_Success() throws SQLException {
        when(queueRepository.reclaimExpiredLeases()).thenReturn(3);

        leaseReclaimer = new LeaseReclaimer(queueRepository, 60);
        leaseReclaimer.reclaimLease();

        verify(queueRepository).reclaimExpiredLeases();
    }

    @Test
    @DisplayName("Should catch and handle SQLException gracefully during reclaimLease")
    void testReclaimLease_HandlesSQLException() throws SQLException {
        doThrow(new SQLException("DB connection failed"))
                .when(queueRepository).reclaimExpiredLeases();

        leaseReclaimer = new LeaseReclaimer(queueRepository, 60);

        assertThatCode(() -> leaseReclaimer.reclaimLease())
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should close cleanly and shut down scheduled executor")
    void testClose() {
        leaseReclaimer = new LeaseReclaimer(queueRepository, 60);
        assertThatCode(() -> leaseReclaimer.close())
                .doesNotThrowAnyException();
    }
}
