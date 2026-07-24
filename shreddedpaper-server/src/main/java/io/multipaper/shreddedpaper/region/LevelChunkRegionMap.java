package io.multipaper.shreddedpaper.region;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.Mob;
import net.minecraft.world.level.BlockEventData;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.piston.MovingPistonBlock;
import net.minecraft.world.level.block.piston.PistonBaseBlock;
import net.minecraft.world.level.chunk.LevelChunk;
import io.multipaper.shreddedpaper.threading.ShreddedPaperRegionLocker;
import io.multipaper.shreddedpaper.util.SimpleStampedLock;
import net.minecraft.world.level.levelgen.structure.BoundingBox;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class LevelChunkRegionMap {

    private static final Logger LOGGER = LogUtils.getClassLogger();

    private final ServerLevel level;
    private final SimpleStampedLock regionsLock = new SimpleStampedLock();
    private final Long2ObjectOpenHashMap<LevelChunkRegion> regions = new Long2ObjectOpenHashMap<>(2048, 0.5f);
    private final AtomicLong blockEventSeqCounter = new AtomicLong();
    public final AtomicLong blockEntityTickerSeqCounter = new AtomicLong(); // See Level#addBlockEntityTicker

    public LevelChunkRegionMap(ServerLevel level) {
        this.level = level;
    }

    public LevelChunkRegion getOrCreate(RegionPos regionPos) {
        LevelChunkRegion levelChunkRegion = get(regionPos);

        if (levelChunkRegion != null) {
            return levelChunkRegion;
        }

        return regionsLock.write(() -> regions.computeIfAbsent(regionPos.longKey, k -> new LevelChunkRegion(level, regionPos)));
    }

    public LevelChunkRegion get(RegionPos regionPos) {
        return regionsLock.optimisticRead(() -> {
            LevelChunkRegion levelChunkRegion = regions.get(regionPos.longKey);
            if (levelChunkRegion != null) {
                levelChunkRegion.bumpLastAccess();
            }
            return levelChunkRegion;
        });
    }

    public void remove(RegionPos regionPos) {
        regionsLock.write(() -> {
            LevelChunkRegion region = regions.remove(regionPos.longKey);
            if (!region.isEmpty()) {
                // Guess this region has been modified by another thread, re-add it
                regions.put(regionPos.longKey, region);
            }
        });
    }

    public void addTickingChunk(LevelChunk levelChunk) {
        getOrCreate(RegionPos.forChunk(levelChunk.getPos())).add(levelChunk);
    }

    public void removeTickingChunk(LevelChunk levelChunk) {
        getOrCreate(RegionPos.forChunk(levelChunk.getPos())).remove(levelChunk);
    }

    public void forEach(Consumer<LevelChunkRegion> consumer) {
        List<LevelChunkRegion> regionsCopy = new ArrayList<>(regions.size());
        regionsLock.read(() -> regionsCopy.addAll(regions.values()));
        regionsCopy.forEach(consumer);
    }

    public void addTickingEntity(Entity entity) {
        if (entity.previousTickingChunkPosRegion != null) {
            throw new IllegalStateException("Entity has already been added to a ticking list " + entity);
        }

        entity.previousTickingChunkPosRegion = entity.chunkPosition();
        getOrCreate(RegionPos.forChunk(entity.chunkPosition())).addTickingEntity(entity);
    }

    public void removeTickingEntity(Entity entity) {
        if (entity.previousTickingChunkPosRegion == null) {
            throw new IllegalStateException("Entity has not been added to a ticking list " + entity);
        }

        getOrCreate(RegionPos.forChunk(entity.previousTickingChunkPosRegion)).removeTickingEntity(entity);
        entity.previousTickingChunkPosRegion = null;
    }

    public void moveTickingEntity(Entity entity) {
        if (entity.previousTickingChunkPosRegion == null) {
            // Not ticking, ignore
            return;
        }

        ChunkPos newChunkPos = entity.chunkPosition();
        RegionPos fromRegion = RegionPos.forChunk(entity.previousTickingChunkPosRegion);
        RegionPos toRegion = RegionPos.forChunk(newChunkPos);

        if (!fromRegion.equals(toRegion)) {
            entity.previousTickingChunkPosRegion = newChunkPos;
            getOrCreate(fromRegion).removeTickingEntity(entity);
            getOrCreate(toRegion).addTickingEntity(entity);
        }
    }

    public void addTrackedEntity(Entity entity) {
        if (entity.previousTrackedChunkPosRegion != null) {
            throw new IllegalStateException("Entity is already tracked " + entity);
        }

        entity.previousTrackedChunkPosRegion = entity.chunkPosition();
        getOrCreate(RegionPos.forChunk(entity.chunkPosition())).addTrackedEntity(entity);

        if (entity instanceof Mob mob) {
            getOrCreate(RegionPos.forChunk(entity.chunkPosition())).addNavigationMob(mob);
        }
    }

    public void removeTrackedEntity(Entity entity) {
        if (entity.previousTrackedChunkPosRegion == null) {
            throw new IllegalStateException("Entity is not being tracked " + entity);
        }

        if (entity instanceof Mob mob) {
            getOrCreate(RegionPos.forChunk(entity.chunkPosition())).removeNavigationMob(mob);
        }

        getOrCreate(RegionPos.forChunk(entity.previousTrackedChunkPosRegion)).removeTrackedEntity(entity);
        entity.previousTrackedChunkPosRegion = null;
    }

    public void moveTrackedEntity(Entity entity) {
        if (entity.previousTrackedChunkPosRegion == null) {
            // Not tracked, ignore
            return;
        }

        ChunkPos newChunkPos = entity.chunkPosition();
        RegionPos fromRegion = RegionPos.forChunk(entity.previousTrackedChunkPosRegion);
        RegionPos toRegion = RegionPos.forChunk(newChunkPos);

        if (!fromRegion.equals(toRegion)) {
            entity.previousTrackedChunkPosRegion = newChunkPos;
            getOrCreate(fromRegion).removeTrackedEntity(entity);
            getOrCreate(toRegion).addTrackedEntity(entity);

            if (entity instanceof Mob mob) {
                getOrCreate(fromRegion).removeNavigationMob(mob);
                getOrCreate(toRegion).addNavigationMob(mob);
            }
        }
    }

    /**
     * Schedule a task to run on the given region's thread at the beginning of the next tick
     */
    public void scheduleTask(RegionPos regionPos, Runnable task) {
        scheduleTask(regionPos, task, 0);
    }

    /**
     * Schedule a task to run on the given region's thread after a certain number of ticks
     */
    public void scheduleTask(RegionPos regionPos, Runnable task, long delayInTicks) {
        getOrCreate(regionPos).scheduleTask(task, delayInTicks);
    }

    /**
     * Execute a task on the given region's thread at the next given opportunity.
     * These tasks must <strong>NOT</strong> modify the chunk (blocks, entities, etc). These
     * tasks must be read-only. Eg loading a chunk, saving data, sending packets, etc.
     */
    public void execute(RegionPos regionPos, Runnable task) {
        getOrCreate(regionPos).getInternalTaskQueue().queueTask(task);
    }

    /**
     * Executor that executes a task on the given region's thread at the next given opportunity.
     * These tasks must <strong>NOT</strong> modify the chunk (blocks, entities, etc). These
     * tasks must be read-only. Eg loading a chunk, saving data, sending packets, etc.
     */
    public Executor executorFor(RegionPos regionPos) {
        return runnable -> execute(regionPos, runnable);
    }

    public void addPlayer(ServerPlayer player) {
        player.previousChunkPosRegion = player.chunkPosition();
        getOrCreate(RegionPos.forChunk(player.chunkPosition())).addPlayer(player);
    }

    public void removePlayer(ServerPlayer player) {
        getOrCreate(RegionPos.forChunk(player.chunkPosition())).removePlayer(player);
    }

    public void movePlayer(ServerPlayer player) {
        RegionPos fromRegion = RegionPos.forChunk(player.previousChunkPosRegion);
        RegionPos toRegion = RegionPos.forChunk(player.chunkPosition());

        if (!fromRegion.equals(toRegion)) {
            player.previousChunkPosRegion = player.chunkPosition();
            getOrCreate(fromRegion).removePlayer(player);
            getOrCreate(toRegion).addPlayer(player);
        }
    }

    public void addBlockEvent(BlockEventData blockEvent) {
        // The sequence counter is level-global so block events queued into different regions
        // keep their relative queueing order - the split-phase drain merges adjacent regions'
        // queues by this sequence to reproduce vanilla's single-FIFO processing order.
        RegionPos centerRegionPos = RegionPos.forBlockPos(blockEvent.pos());
        LevelChunkRegion centerRegion = getOrCreate(centerRegionPos);
        centerRegion.addBlockEvent(blockEvent, this.blockEventSeqCounter);

        // Only piston-family events are order-sensitive across region borders - container lid
        // animations, note blocks and bells also arrive here but can run in any relative order,
        // and must not mark regions for merged ticking or ambient container traffic chains much
        // of the map into one serialized cluster.
        Block block = blockEvent.block();
        if (!(block instanceof PistonBaseBlock || block instanceof MovingPistonBlock)) {
            return;
        }

        // Record a border edge with every region within a piston's reach (16 blocks) of the event,
        // including neighbours the contraption is about to expand into, so the split-phase ticker
        // merges exactly the regions whose shared border the contraption is working across. The
        // 33x33 block square around the event spans at most 2x2 regions, so its corners cover all
        // of them. An interior piston clock records no edges and merges nothing.
        long gameTime = level.getGameTime();
        for (int dx = -16; dx <= 16; dx += 32) {
            for (int dz = -16; dz <= 16; dz += 32) {
                RegionPos nearbyRegion = RegionPos.forBlockPos(blockEvent.pos().getX() + dx, 0, blockEvent.pos().getZ() + dz);
                if (nearbyRegion.longKey != centerRegionPos.longKey) {
                    centerRegion.stampBorderEdge(nearbyRegion.longKey, gameTime);
                }
            }
        }
    }

    public void forEachRegionInBoundingBox(BoundingBox box, Consumer<LevelChunkRegion> consumer) {
        RegionPos minPos = RegionPos.forBlockPos(box.minX(), box.minZ(), box.minZ());
        RegionPos maxPos = RegionPos.forBlockPos(box.maxX(), box.maxZ(), box.maxZ());

        for (int x = minPos.x; x <= maxPos.x; x++) {
            for (int z = minPos.z; z <= maxPos.z; z++) {
                LevelChunkRegion region = get(new RegionPos(x, z));
                if (region != null) {
                    consumer.accept(region);
                }
            }
        }
    }

    public List<Mob> collectRelevantNavigatingMobs(RegionPos regionPos) {
        if (!level.chunkScheduler.getRegionLocker().hasLock(regionPos)) {
            // We care about the navigating mobs in at least this region, ensure it's locked
            throw new IllegalStateException("Collecting navigating mobs outside of region's thread");
        }

        ObjectArrayList<Mob> navigatingMobs = new ObjectArrayList<>();

        for (int x = -ShreddedPaperRegionLocker.REGION_LOCK_RADIUS; x <= ShreddedPaperRegionLocker.REGION_LOCK_RADIUS; x++) {
            for (int z = -ShreddedPaperRegionLocker.REGION_LOCK_RADIUS; z <= ShreddedPaperRegionLocker.REGION_LOCK_RADIUS; z++) {
                RegionPos i = new RegionPos(regionPos.x + x, regionPos.z + z);

                // Only collect mobs from regions that are locked - if it's not locked, it should be too far away to matter
                if (!level.chunkScheduler.getRegionLocker().hasLock(i)) continue;

                LevelChunkRegion region = get(i);
                if (region == null) continue;

                region.collectNavigatingMobs(navigatingMobs);
            }
        }

        return navigatingMobs;
    }
}
