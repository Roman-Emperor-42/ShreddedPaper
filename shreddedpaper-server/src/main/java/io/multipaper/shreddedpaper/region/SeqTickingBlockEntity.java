package io.multipaper.shreddedpaper.region;

import net.minecraft.core.BlockPos;
import net.minecraft.world.level.block.entity.TickingBlockEntity;

/**
 * Wraps a block entity ticker with a level-global registration sequence. Vanilla ticks all
 * block entities of a level through one list in insertion order; ShreddedPaper splits that
 * list per region, losing the cross-region relative order. The sequence lets the split-phase
 * cluster drain merge adjacent regions' ticker lists back into vanilla's global list order,
 * which matters for contraptions straddling a region boundary (two moving-piston block
 * entities finishing the same tick must place their blocks - and thereby schedule observer
 * pulses - in the same relative order as vanilla).
 */
public record SeqTickingBlockEntity(TickingBlockEntity delegate, long seq) implements TickingBlockEntity {

    @Override
    public void tick() {
        this.delegate.tick();
    }

    @Override
    public boolean isRemoved() {
        return this.delegate.isRemoved();
    }

    @Override
    public BlockPos getPos() {
        return this.delegate.getPos();
    }

    @Override
    public String getType() {
        return this.delegate.getType();
    }
}
