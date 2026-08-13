package io.appform.dropwizard.sharding.sanity.tests;

import io.appform.dropwizard.sharding.sanity.base.SanityTestBase;
import io.appform.dropwizard.sharding.sanity.entities.SanityOrderItem;
import lombok.val;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Restrictions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sanity tests for {@code RelationalDao.saveAll()} covering:
 * <ul>
 *   <li>TC-1: Successful batch save of 5 items + cross-connection read-back of all 5.</li>
 *   <li>TC-2: Batch save where the 3rd item causes a constraint violation →
 *             all 5 must be rolled back (none visible on read).</li>
 * </ul>
 *
 * <p>Both tests follow the <b>blanket-read</b> pattern: read via the reader bundle before
 * and after writes to verify cross-connection consistency.
 */
class SaveAllSanityTest extends SanityTestBase {

    @BeforeEach
    void cleanTables() throws Exception {
        truncateAllTables();
    }

    // -------------------------------------------------------------------------
    // TC-1: SaveAll (5 items) + read all back via reader bundle
    // -------------------------------------------------------------------------

    @Test
    void saveAll_fiveItems_allVisibleOnCrossConnectionRead() throws Exception {
        val orderId = UUID.randomUUID().toString();

        // 1. Blanket read — nothing should exist yet
        val beforeItems = selectItemsByOrderId(orderId);
        assertTrue(beforeItems.isEmpty(), "No items should exist before saveAll");

        // 2. Build 5 items
        val items = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> SanityOrderItem.builder()
                        .orderId(orderId)
                        .itemName("item-" + i)
                        .quantity(i)
                        .price(i * 100)
                        .build())
                .collect(Collectors.toList());

        // 3. SaveAll via writer bundle
        boolean saved = writerOrderItemDao.saveAll(orderId, items);
        assertTrue(saved, "saveAll should return true on success");

        // 4. Read back via READER bundle (guaranteed different connection pool)
        val afterItems = selectItemsByOrderId(orderId);
        assertEquals(5, afterItems.size(),
                "All 5 items must be visible on cross-connection read");

        // 5. Verify data integrity — all item names present with correct fields
        for (int i = 1; i <= 5; i++) {
            val itemName = "item-" + i;
            val match = afterItems.stream()
                    .filter(it -> itemName.equals(it.getItemName()))
                    .findFirst();
            assertTrue(match.isPresent(), "Item '" + itemName + "' must be present");
            assertEquals(i, match.get().getQuantity());
            assertEquals(i * 100, match.get().getPrice());
            assertEquals(orderId, match.get().getOrderId());
        }
    }

    // -------------------------------------------------------------------------
    // TC-2: SaveAll with exception at 3rd item → full rollback
    // -------------------------------------------------------------------------

    @Test
    void saveAll_exceptionAtThirdItem_allRolledBack() throws Exception {
        val orderId = UUID.randomUUID().toString();

        // 1. Blanket read — nothing should exist
        val beforeItems = selectItemsByOrderId(orderId);
        assertTrue(beforeItems.isEmpty(), "No items should exist before saveAll");

        // 2. First, insert item-3 so that the batch will hit a unique constraint
        //    violation on (order_id, item_name) when the batch tries to insert item-3 again.
        writerOrderItemDao.save(orderId, SanityOrderItem.builder()
                .orderId(orderId)
                .itemName("item-3")
                .quantity(99)
                .price(9999)
                .build());

        // Verify the pre-inserted item exists via reader
        val preInserted = selectItemsByOrderId(orderId);
        assertEquals(1, preInserted.size(), "Pre-inserted item-3 should exist");

        // 3. Build batch of 5 items — item-3 will collide with the pre-inserted row
        val items = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> SanityOrderItem.builder()
                        .orderId(orderId)
                        .itemName("item-" + i)
                        .quantity(i)
                        .price(i * 100)
                        .build())
                .collect(Collectors.toList());

        // 4. SaveAll should fail due to unique constraint violation on item-3
        assertThrows(Exception.class, () -> writerOrderItemDao.saveAll(orderId, items),
                "saveAll must throw when a constraint violation occurs");

        // 5. Read back via READER bundle — only the original pre-inserted item-3
        //    should exist. The batch items (1-5) must all be rolled back.
        val afterItems = selectItemsByOrderId(orderId);
        assertEquals(1, afterItems.size(),
                "Only the pre-inserted item-3 should remain; batch must be fully rolled back");
        assertEquals("item-3", afterItems.get(0).getItemName());
        assertEquals(99, afterItems.get(0).getQuantity(),
                "Pre-inserted item-3 should retain its original data, not the batch version");
    }

    // -------------------------------------------------------------------------
    // Helper: read items from READER bundle by orderId
    // -------------------------------------------------------------------------

    private List<SanityOrderItem> selectItemsByOrderId(String orderId) throws Exception {
        val criteria = DetachedCriteria.forClass(SanityOrderItem.class)
                .add(Restrictions.eq("orderId", orderId));
        return readerOrderItemDao.select(orderId, criteria, 0, Integer.MAX_VALUE);
    }
}
