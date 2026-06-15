package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

/**
 * Observer that handles {@code @CopyFromParent} field processing for child entities
 * saved within a {@link io.appform.dropwizard.sharding.dao.LockedContext}.
 * <p>
 * This observer is opt-in. Register it during bundle initialization:
 * <pre>
 *     dbShardingBundle.registerObserver(CopyFromParentObserver.builder()
 *         .copyEnabled(true)
 *         .mismatchDetectionEnabled(true)
 *         .mismatchListener(() -&gt; myMismatchListener)
 *         .build());
 * </pre>
 * <p>
 * Configuration options:
 * <ul>
 *     <li>{@code copyEnabled} — whether to copy {@code @CopyFromParent(override=true)} fields
 *         from parent to child before persistence (default: {@code true})</li>
 *     <li>{@code copyIfDefaultOnly} — when {@code true}, copy only if child field has default value,
 *         otherwise log error. Used for migration validation. (default: {@code false})</li>
 *     <li>{@code mismatchDetectionEnabled} — whether to detect mismatches between parent and
 *         child field values before copying (default: {@code false})</li>
 *     <li>{@code mismatchListener} — lazy supplier for the callback invoked when mismatches
 *         are detected. Required when {@code mismatchDetectionEnabled} is {@code true}.
 *         The supplier is resolved once on the first database transaction.</li>
 * </ul>
 *
 * @see CopyFromParentMismatchListener
 * @see io.appform.dropwizard.sharding.utils.CopyFromParentUtils
 */
@Slf4j
public class CopyFromParentObserver extends TransactionObserver {

    private final boolean copyEnabled;
    private final boolean mismatchDetectionEnabled;
    private final boolean copyIfDefaultOnly;
    private final Supplier<CopyFromParentMismatchListener> mismatchListenerSupplier;
    private volatile CopyFromParentPersistor persistor;

    private CopyFromParentObserver(boolean copyEnabled,
                                   boolean mismatchDetectionEnabled,
                                   boolean copyIfDefaultOnly,
                                   Supplier<CopyFromParentMismatchListener> mismatchListenerSupplier) {
        super(null);
        this.copyEnabled = copyEnabled;
        this.mismatchDetectionEnabled = mismatchDetectionEnabled;
        this.copyIfDefaultOnly = copyIfDefaultOnly;
        this.mismatchListenerSupplier = mismatchListenerSupplier;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        context.getOpContext().visit(getPersistor());
        return proceed(context, supplier);
    }

    private CopyFromParentPersistor getPersistor() {
        if (persistor == null) {
            CopyFromParentMismatchListener listener = null;
            if (mismatchDetectionEnabled && mismatchListenerSupplier != null) {
                try {
                    listener = mismatchListenerSupplier.get();
                } catch (Exception e) {
                    log.error("Failed to resolve CopyFromParentMismatchListener, "
                            + "mismatch detection will be disabled", e);
                }
            }
            persistor = new CopyFromParentPersistor(copyEnabled, mismatchDetectionEnabled, 
                    copyIfDefaultOnly, listener);
        }
        return persistor;
    }

    public static class Builder {
        private boolean copyEnabled = true;
        private boolean mismatchDetectionEnabled = false;
        private boolean copyIfDefaultOnly = false;
        private Supplier<CopyFromParentMismatchListener> mismatchListenerSupplier;

        private Builder() {}

        /**
         * Enable or disable copying of {@code @CopyFromParent(override=true)} fields
         * from parent to child before persistence. Default: {@code true}.
         */
        public Builder copyEnabled(boolean copyEnabled) {
            this.copyEnabled = copyEnabled;
            return this;
        }

        /**
         * Enable or disable detection of field value mismatches between parent and child.
         * When enabled, a {@link #mismatchListener} must also be provided.
         * Default: {@code false}.
         */
        public Builder mismatchDetectionEnabled(boolean mismatchDetectionEnabled) {
            this.mismatchDetectionEnabled = mismatchDetectionEnabled;
            return this;
        }

        /**
         * Enable strict default-value checking during copy.
         * When {@code true}, fields are only copied if the child field has a default value
         * (null for references, 0/false for primitives). If a non-default value is found,
         * an error is logged and the copy is skipped for that field.
         * <p>
         * This is intended for migration validation to detect lingering manual setters.
         * After migration is complete, this should be set to {@code false}.
         * Default: {@code false}.
         */
        public Builder copyIfDefaultOnly(boolean copyIfDefaultOnly) {
            this.copyIfDefaultOnly = copyIfDefaultOnly;
            return this;
        }

        /**
         * Lazy supplier for the mismatch listener. Resolved once on first database transaction.
         * <p>
         * Use a lazy supplier to defer resolution until after framework initialization
         * (e.g. after a Guice injector is created):
         * <pre>
         *     .mismatchListener(() -&gt; guiceBundle.getInjector().getInstance(MyListener.class))
         * </pre>
         */
        public Builder mismatchListener(Supplier<CopyFromParentMismatchListener> mismatchListenerSupplier) {
            this.mismatchListenerSupplier = mismatchListenerSupplier;
            return this;
        }

        public CopyFromParentObserver build() {
            if (mismatchDetectionEnabled && mismatchListenerSupplier == null) {
                throw new IllegalArgumentException(
                        "mismatchListener must be provided when mismatchDetectionEnabled is true");
            }
            return new CopyFromParentObserver(copyEnabled, mismatchDetectionEnabled,
                    copyIfDefaultOnly, mismatchListenerSupplier);
        }
    }
}
