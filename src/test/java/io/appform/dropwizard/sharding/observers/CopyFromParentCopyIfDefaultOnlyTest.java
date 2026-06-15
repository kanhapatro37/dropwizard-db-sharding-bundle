package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.dao.operations.SaveWithParent;
import io.appform.dropwizard.sharding.execution.DaoType;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.sharding.CopyFromParent;
import io.appform.dropwizard.sharding.sharding.ParentEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the copyIfDefaultOnly flag.
 * This flag is used during migration rollout to detect lingering manual setters.
 */
class CopyFromParentCopyIfDefaultOnlyTest {

    // Test entities
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class TestParent {
        private String transactionId;
        private long amount;
        private String customerId;
        private Integer nullableField;
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

        @CopyFromParent(field = "customerId")
        private String customerId;

        @CopyFromParent(field = "nullableField")
        private Integer nullableField;

        private String ownField;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class PrimitiveParent {
        private int intVal;
        private long longVal;
        private boolean boolVal;
        private double doubleVal;
        private char charVal;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ParentEntity(PrimitiveParent.class)
    static class PrimitiveChild {
        @CopyFromParent(field = "intVal")
        private int intVal;

        @CopyFromParent(field = "longVal")
        private long longVal;

        @CopyFromParent(field = "boolVal")
        private boolean boolVal;

        @CopyFromParent(field = "doubleVal")
        private double doubleVal;

        @CopyFromParent(field = "charVal")
        private char charVal;
    }

    // Test: copyIfDefaultOnly=true with all default values (happy path)

    @Test
    void testCopyIfDefaultOnly_allDefaults_copiesSuccessfully() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)  // Strict mode
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("TXN-123")
                .amount(1000L)
                .customerId("CUST-1")
                .nullableField(42)
                .build();

        // Child has all default values
        TestChild child = new TestChild();  

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // All fields should be copied
        assertEquals("TXN-123", result.getTxnId());
        assertEquals(1000L, result.getChildAmount());
        assertEquals("CUST-1", result.getCustomerId());
        assertEquals(42, result.getNullableField());
    }

    // Test: copyIfDefaultOnly=true with non-default value (manual setter detected)

    @Test
    void testCopyIfDefaultOnly_nonDefaultString_skipsAndLogs() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(1000L)
                .customerId("PARENT-CUST")
                .build();

        TestChild child = TestChild.builder()
                .txnId("MANUAL-SETTER-VALUE")  // Non-default! Manual setter detected
                .childAmount(0L)  // Default
                .customerId(null)  // Default
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // txnId should NOT be copied (has manual setter value)
        assertEquals("MANUAL-SETTER-VALUE", result.getTxnId(), 
                "Should preserve manual setter value and skip copy");

        // Other defaults should be copied
        assertEquals(1000L, result.getChildAmount());
        assertEquals("PARENT-CUST", result.getCustomerId());
    }

    @Test
    void testCopyIfDefaultOnly_nonDefaultPrimitive_skipsAndLogs() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("TXN")
                .amount(1000L)
                .customerId("CUST")
                .build();

        TestChild child = TestChild.builder()
                .txnId(null)  // Default
                .childAmount(999L)  // Non-default! Manual setter detected
                .customerId(null)  // Default
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // amount should NOT be copied (has manual setter value)
        assertEquals(999L, result.getChildAmount(), 
                "Should preserve manual setter value and skip copy");

        // Other defaults should be copied
        assertEquals("TXN", result.getTxnId());
        assertEquals("CUST", result.getCustomerId());
    }

    // Test: copyIfDefaultOnly=true with mixed defaults and non-defaults

    @Test
    void testCopyIfDefaultOnly_mixedValues_copiesOnlyDefaults() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(1000L)
                .customerId("PARENT-CUST")
                .nullableField(42)
                .build();

        TestChild child = TestChild.builder()
                .txnId("MANUAL-1")      // Non-default - skip
                .childAmount(0L)         // Default - copy
                .customerId("MANUAL-2")  // Non-default - skip
                .nullableField(null)     // Default - copy
                .ownField("preserved")   // Not annotated - ignore
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Non-defaults should be preserved (manual setters detected)
        assertEquals("MANUAL-1", result.getTxnId());
        assertEquals("MANUAL-2", result.getCustomerId());

        // Defaults should be copied
        assertEquals(1000L, result.getChildAmount());
        assertEquals(42, result.getNullableField());

        // Non-annotated field preserved
        assertEquals("preserved", result.getOwnField());
    }

    // Test: copyIfDefaultOnly=false (normal mode)

    @Test
    void testCopyIfDefaultOnly_false_copiesEverything() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(false)  // Normal mode
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(1000L)
                .customerId("PARENT-CUST")
                .build();

        TestChild child = TestChild.builder()
                .txnId("OLD-VALUE")      // Will be overwritten
                .childAmount(999L)        // Will be overwritten
                .customerId("OLD-CUST")   // Will be overwritten
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // All fields should be overwritten (normal copy behavior)
        assertEquals("PARENT-TXN", result.getTxnId());
        assertEquals(1000L, result.getChildAmount());
        assertEquals("PARENT-CUST", result.getCustomerId());
    }

    // Test: Primitive default value detection

    @Test
    void testCopyIfDefaultOnly_allPrimitivesDefault_copies() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        PrimitiveParent parent = PrimitiveParent.builder()
                .intVal(42)
                .longVal(999L)
                .boolVal(true)
                .doubleVal(3.14)
                .charVal('X')
                .build();

        PrimitiveChild child = new PrimitiveChild();  // All defaults: 0, 0L, false, 0.0, '\0'

        SaveWithParent<PrimitiveChild, PrimitiveChild, PrimitiveParent> opContext =
                SaveWithParent.<PrimitiveChild, PrimitiveChild, PrimitiveParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        PrimitiveChild result = observer.execute(ctx, () -> opContext.apply(null));

        assertEquals(42, result.getIntVal());
        assertEquals(999L, result.getLongVal());
        assertTrue(result.isBoolVal());
        assertEquals(3.14, result.getDoubleVal(), 0.001);
        assertEquals('X', result.getCharVal());
    }

    @Test
    void testCopyIfDefaultOnly_primitivesNonDefault_skips() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        PrimitiveParent parent = PrimitiveParent.builder()
                .intVal(42)
                .longVal(999L)
                .boolVal(true)
                .doubleVal(3.14)
                .charVal('X')
                .build();

        PrimitiveChild child = PrimitiveChild.builder()
                .intVal(7)           // Non-default - skip
                .longVal(0L)         // Default - copy
                .boolVal(true)       // Non-default (true != false) - skip
                .doubleVal(0.0)      // Default - copy
                .charVal('A')        // Non-default - skip
                .build();

        SaveWithParent<PrimitiveChild, PrimitiveChild, PrimitiveParent> opContext =
                SaveWithParent.<PrimitiveChild, PrimitiveChild, PrimitiveParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        PrimitiveChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Non-defaults preserved
        assertEquals(7, result.getIntVal());
        assertTrue(result.isBoolVal());
        assertEquals('A', result.getCharVal());

        // Defaults copied
        assertEquals(999L, result.getLongVal());
        assertEquals(3.14, result.getDoubleVal(), 0.001);
    }

    // Test: copyEnabled=false with copyIfDefaultOnly=true (validation only)

    @Test
    void testCopyDisabled_withCopyIfDefaultOnly_noEffect() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(false)  // Copy disabled
                .copyIfDefaultOnly(true)  // This should have no effect
                .build();

        TestParent parent = TestParent.builder()
                .transactionId("PARENT-TXN")
                .amount(1000L)
                .build();

        TestChild child = TestChild.builder()
                .txnId("OLD-VALUE")
                .childAmount(999L)
                .build();

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Nothing should be copied (copyEnabled=false)
        assertEquals("OLD-VALUE", result.getTxnId());
        assertEquals(999L, result.getChildAmount());
    }

    // Test: Null parent values with copyIfDefaultOnly

    @Test
    void testCopyIfDefaultOnly_nullParentValue_copies() {
        CopyFromParentObserver observer = CopyFromParentObserver.builder()
                .copyEnabled(true)
                .copyIfDefaultOnly(true)
                .build();

        TestParent parent = TestParent.builder()
                .transactionId(null)  // Null parent value
                .amount(0L)           // Zero parent value
                .build();

        TestChild child = new TestChild();  // All defaults

        SaveWithParent<TestChild, TestChild, TestParent> opContext =
                SaveWithParent.<TestChild, TestChild, TestParent>builder()
                        .entity(child)
                        .parent(parent)
                        .saver(e -> e)
                        .build();

        TransactionExecutionContext ctx = createContext(opContext);
        TestChild result = observer.execute(ctx, () -> opContext.apply(null));

        // Null and zero values should still be copied
        assertNull(result.getTxnId());
        assertEquals(0L, result.getChildAmount());
    }

    private <R> TransactionExecutionContext createContext(
            io.appform.dropwizard.sharding.dao.operations.OpContext<R> opContext) {
        return TransactionExecutionContext.builder()
                .opContext(opContext)
                .commandName("testCommand")
                .daoType(DaoType.RELATIONAL)
                .entityClass(TestChild.class)
                .shardName("shard-0")
                .build();
    }
}
