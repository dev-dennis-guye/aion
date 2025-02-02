package org.aion.vm.exception;

import org.aion.vm.common.BulkExecutor;

/**
 * @implNote The VMException is an Exception throws from the BulkExecutor {@link
 *     BulkExecutor} when the vm has fatal situation occurs,e.g. OOM. The kernel will
 *     handle this exception by shutting down the kernel immediately.
 * @author Jay Tseng
 */
public class VMException extends Exception {

    /** @param result the transaction executing result returned by the VM. */
    public VMException(String result) {
        super(result);
    }
}
