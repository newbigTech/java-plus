package org.java.plus.dag.core.base.em;

/**
 * all Enum class need implements this interface
 *
 * @author
 */
public interface EnumInterface {
    /**
     * hash code
     *
     * @return
     */
    default int getIdentify() {
        return 31 * name().hashCode() + getClass().getName().hashCode();
    }

    /**
     * Enum name
     *
     * @return
     */
    String name();

}
