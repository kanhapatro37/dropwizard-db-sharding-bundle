package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.dao.operations.Save;
import io.appform.dropwizard.sharding.dao.operations.SaveWithParent;
import io.appform.dropwizard.sharding.execution.DaoType;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.sharding.CopyFromParent;
import io.appform.dropwizard.sharding.sharding.ParentEntity;
import io.appform.dropwizard.sharding.utils.CopyFromParentUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CopyFromParentObserverTest {

    // Test entities
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestParent {
        private String transactionId;
        private long amount;
        private String customerId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ParentEntity(TestParent.class)
    static class TestChild {
        @CopyFromParent(field = "transactionId")
        private String txnId;

        @CopyFromParent(field = "amount")
        private long childAmount;

        private String ownField;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ParentEntity(TestParent.class)
    static class MismatchChild {
        @CopyFromParent(field = "transactionId")
        private String txnId;

        @CopyFromParent(field = "amount")
        private long childAmount;
    }

    // Test: copyEnabled=true, mismatchDetectionEnabled=false (default behavior)

    @Test
    void testDefaultConfig_copiesFields_noMismatchDetection() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(false)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("TXN-123")
                .amount(500)
                .build();
        TestChild child = TestChild.builder()
                .ownField("mine")
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);

        AtomicBoolean supplierCalled = new AtomicBoolean(false);
        TestChild result = observer.execute(ctx, () -> {
            supplierCalled.set(true);
            return opContext.apply(null);
        });

        assertTrue(supplierCalled.get());
        assertEquals("TXN-123", result.getTxnId(), "Field should be copied from parent");
        assertEquals(500, result.getChildAmount(), "Field should be copied from parent");
        assertEquals("mine", result.getOwnField(), "Non-annotated field should not be touched");
    }

    @Test
    void testDefaultConfig_overwritesExistingValues() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(false)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(999)
                .build();
        TestChild child = TestChild.builder()
                .txnId("OLD-TXN")
                .childAmount(1)
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        assertEquals("PARENT-TXN", result.getTxnId(), "Should overwrite existing value");
        assertEquals(999, result.getChildAmount(), "Should overwrite existing value");
    }

    @Test
    void testCopyAndDetect_copiesFields_detectsMismatches() {
        CopyFromParentMismatchListener mockListener = mock(CopyFromParentMismatchListener.class);

        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> mockListener)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(500)
                .build();
        MismatchChild child = MismatchChild.builder()
                .txnId("CHILD-TXN")  // Mismatch
                .childAmount(100)     // Mismatch
                .build();

        SaveWithParent<MismatchChild, MismatchChild, TestParent> opContext =
                SaveWithParent.<MismatchChild, MismatchChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        MismatchChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Verify mismatch detection was called
        ArgumentCaptor<List<CopyFromParentUtils.FieldMismatch>> mismatchCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(mockListener).onMismatches(eq(parent), eq(child), mismatchCaptor.capture());

        List<CopyFromParentUtils.FieldMismatch> mismatches = mismatchCaptor.getValue();
        assertEquals(2, mismatches.size(), "Should detect 2 mismatches");

        // Verify fields were still copied (mismatch detection doesn't prevent copy)
        assertEquals("PARENT-TXN", result.getTxnId());
        assertEquals(500, result.getChildAmount());
    }

    @Test
    void testCopyAndDetect_noMismatchesDetected_listenerNotCalled() {
        CopyFromParentMismatchListener mockListener = mock(CopyFromParentMismatchListener.class);

        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> mockListener)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("SAME-TXN")
                .amount(100)
                .build();
        MismatchChild child = MismatchChild.builder()
                .txnId("SAME-TXN")  // Matches parent
                .childAmount(100)    // Matches parent
                .build();

        SaveWithParent<MismatchChild, MismatchChild, TestParent> opContext =
                SaveWithParent.<MismatchChild, MismatchChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        observer.execute(ctx, () -> opContext.apply(null));

        // Listener should not be called when there are no mismatches
        verify(mockListener, never()).onMismatches(any(), any(), any());
    }

    @Test
    void testCopyAndDetect_listenerExceptionDoesNotBreakCopy() {
        CopyFromParentMismatchListener mockListener = mock(CopyFromParentMismatchListener.class);
        doThrow(new RuntimeException("Listener explosion"))
                .when(mockListener).onMismatches(any(), any(), any());

        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> mockListener)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(500)
                .build();
        MismatchChild child = MismatchChild.builder()
                .txnId("CHILD-TXN")
                .childAmount(100)
                .build();

        SaveWithParent<MismatchChild, MismatchChild, TestParent> opContext =
                SaveWithParent.<MismatchChild, MismatchChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);

        // Should not throw - listener exception should be caught
        MismatchChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Fields should still be copied despite listener failure
        assertEquals("PARENT-TXN", result.getTxnId());
        assertEquals(500, result.getChildAmount());
    }

    @Test
    void testNoCopyNoDetect_doesNotCopyFields() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(false)
                .mismatchDetectionEnabled(false)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(500)
                .build();
        TestChild child = TestChild.builder()
                .txnId("ORIGINAL-TXN")
                .childAmount(100)
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Fields should NOT be copied
        assertEquals("ORIGINAL-TXN", result.getTxnId(), "Field should not be copied");
        assertEquals(100, result.getChildAmount(), "Field should not be copied");
    }

    @Test
    void testValidationMode_detectsButDoesNotCopy() {
        CopyFromParentMismatchListener mockListener = mock(CopyFromParentMismatchListener.class);

        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(false)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> mockListener)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(500)
                .build();
        MismatchChild child = MismatchChild.builder()
                .txnId("CHILD-TXN")
                .childAmount(100)
                .build();

        SaveWithParent<MismatchChild, MismatchChild, TestParent> opContext =
                SaveWithParent.<MismatchChild, MismatchChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        MismatchChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Verify mismatch detection was called
        verify(mockListener).onMismatches(eq(parent), eq(child), any());

        // But fields should NOT be copied
        assertEquals("CHILD-TXN", result.getTxnId(), "Field should not be copied in validation mode");
        assertEquals(100, result.getChildAmount(), "Field should not be copied in validation mode");
    }

    @Test
    void testLazyListenerInit_supplierCalledOnlyOnce() {
        AtomicInteger supplierCallCount = new AtomicInteger(0);
        CopyFromParentMismatchListener mockListener = mock(CopyFromParentMismatchListener.class);

        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> {
                    supplierCallCount.incrementAndGet();
                    return mockListener;
                })
                .build();

        TestParent parent = TestParent.builder().transactionId("TXN").amount(100).build();
        MismatchChild child = MismatchChild.builder().txnId("DIFF").childAmount(50).build();

        SaveWithParent<MismatchChild, MismatchChild, TestParent> opContext =
                SaveWithParent.<MismatchChild, MismatchChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);

        // Execute multiple times
        observer.execute(ctx, () -> opContext.apply(null));
        observer.execute(ctx, () -> opContext.apply(null));
        observer.execute(ctx, () -> opContext.apply(null));

        // Supplier should be called only once (lazy init + caching)
        assertEquals(1, supplierCallCount.get(), "Supplier should be called only once");
    }

    @Test
    void testLazyListenerInit_supplierFailure_copyContinues() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> {
                    throw new RuntimeException("Guice not initialized");
                })
                .build();

        TestParent parent = TestParent.builder().transactionId("TXN").amount(100).build();
        TestChild child = TestChild.builder().build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);

        // Should not throw - supplier failure should be caught and logged
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Copy should still work
        assertEquals("TXN", result.getTxnId());
        assertEquals(100, result.getChildAmount());
    }

    @Test
    void testNonSaveWithParent_proceedsWithoutError() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .mismatchDetectionEnabled(true)
                .mismatchListener(() -> mock(CopyFromParentMismatchListener.class))
                .build();

        Save<String, String> saveOp = Save.<String, String>builder()
                .entity("entity")
                .saver(e -> e)
                .build();

        TransactionExecutionContext ctx = createContext(saveOp);

        String result = observer.execute(ctx, () -> "done");

        assertEquals("done", result);
    }

    // Test: Builder validation
    @Test
    void testBuilder_mismatchDetectionEnabledWithoutListener_throwsException() {
        assertThrows(IllegalArgumentException.class, () ->
                CopyFromParentObserver.builder()
                        .copyEnabled(true)
                        .mismatchDetectionEnabled(true)
                        .build(),
                "Should throw when mismatchDetectionEnabled=true but listener is null");
    }

    @Test
    void testBuilder_mismatchDetectionDisabled_listenerNotRequired() {
        assertDoesNotThrow(() ->
                CopyFromParentObserver.builder()
                        .copyEnabled(true)
                        .mismatchDetectionEnabled(false)
                        .build(),
                "Should not require listener when mismatchDetectionEnabled=false");
    }

    @Test
    void testBuilder_defaultValues() {
        // Default: copyEnabled=true, mismatchDetectionEnabled=false
        CopyFromParentObserver observer = CopyFromParentObserver.builder().build();

        TestParent parent = TestParent.builder().transactionId("TXN").amount(100).build();
        TestChild child = TestChild.builder().build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Should copy by default
        assertEquals("TXN", result.getTxnId());
        assertEquals(100, result.getChildAmount());
    }

    @Test
    void testMultipleFieldsCopied() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("TXN-999")
                .amount(12345)
                .customerId("CUST-ABC")
                .build();
        TestChild child = TestChild.builder()
                .ownField("preserved")
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        assertEquals("TXN-999", result.getTxnId());
        assertEquals(12345, result.getChildAmount());
        assertEquals("preserved", result.getOwnField());
    }

    @Test
    void testNullParentValues_copiedCorrectly() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId(null)  // Null value
                .amount(0)            // Zero value
                .build();
        TestChild child = TestChild.builder()
                .txnId("EXISTING")
                .childAmount(999)
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        assertNull(result.getTxnId(), "Null value should be copied");
        assertEquals(0, result.getChildAmount(), "Zero value should be copied");
    }

    @Test
    void testAfterSaveFunction_executedAfterCopy() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("TXN")
                .amount(100)
                .build();
        TestChild child = TestChild.builder().build();

        AtomicBoolean afterSaveCalled = new AtomicBoolean(false);

        SaveWithParent<TestChild, String, TestParent> opContext =
                SaveWithParent.<TestChild, String, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .afterSave(e -> {
                            afterSaveCalled.set(true);
                            // Verify fields are copied before afterSave is called
                            assertEquals("TXN", e.getTxnId());
                            assertEquals(100, e.getChildAmount());
                            return "transformed";
                        })
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        String result = observer.execute(ctx, () -> opContext.apply(null));

        assertTrue(afterSaveCalled.get());
        assertEquals("transformed", result);
    }

    private <R> TransactionExecutionContext createContext(io.appform.dropwizard.sharding.dao.operations.OpContext<R> opContext) {
        return TransactionExecutionContext.builder()
                .opContext(opContext)
                .commandName("testCommand")
                .daoType(DaoType.RELATIONAL)
                .entityClass(TestChild.class)
                .shardName("shard-0")
                .build();
    }
}
