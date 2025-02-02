package org.aion.zero.impl.valid;

import java.util.LinkedList;
import java.util.List;
import org.aion.mcf.blockchain.BlockHeader;
import org.slf4j.Logger;

/** validation rules depending on parent's block header */
public class ParentBlockHeaderValidator
        extends AbstractBlockHeaderValidator {

    private List<DependentBlockHeaderRule> rules;

    public ParentBlockHeaderValidator(List<DependentBlockHeaderRule> rules) {
        this.rules = rules;
    }

    public boolean validate(BlockHeader header, BlockHeader parent, Logger logger) {
        List<RuleError> errors = new LinkedList<>();

        for (DependentBlockHeaderRule rule : rules) {
            if (!rule.validate(header, parent, errors)) {
                if (logger != null) logErrors(logger, errors);
                return false;
            }
        }
        return true;
    }
}
