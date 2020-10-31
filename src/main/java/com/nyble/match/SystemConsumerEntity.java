package com.nyble.match;

import java.util.Objects;

public class SystemConsumerEntity {

    public int systemId;
    public int consumerId;
    public int entityId;
    public boolean needToUpdate;

    public SystemConsumerEntity(int systemId, int consumerId, int entityId) {
        this.systemId = systemId;
        this.consumerId = consumerId;
        this.entityId = entityId;
    }

    public SystemConsumerEntity(int systemId, int consumerId, int newEntityId, boolean b) {
        this(systemId, consumerId, newEntityId);
        needToUpdate = b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SystemConsumerEntity)) return false;
        SystemConsumerEntity that = (SystemConsumerEntity) o;
        return systemId == that.systemId &&
                consumerId == that.consumerId &&
                entityId == that.entityId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemId, consumerId, entityId);
    }
}
