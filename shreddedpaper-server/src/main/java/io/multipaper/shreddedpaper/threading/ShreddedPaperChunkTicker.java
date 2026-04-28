package io.multipaper.shreddedpaper.threading;

import ca.spottedleaf.moonrise.common.util.WorldUtil;
import com.google.common.collect.Lists;
import com.mojang.logging.LogUtils;
import io.multipaper.shreddedpaper.config.ShreddedPaperConfiguration;
import io.multipaper.shreddedpaper.region.RegionPos;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerChunkCache;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.MobCategory;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.NaturalSpawner;
import net.minecraft.world.level.chunk.LevelChunk;
import io.multipaper.shreddedpaper.region.LevelChunkRegion;
import org.bukkit.craftbukkit.entity.CraftEntity;
import org.bukkit.craftbukkit.util.CraftSpawnCategory;
import org.bukkit.entity.SpawnCategory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ShreddedPaperChunkTicker {

    private static final Logger LOGGER = LogUtils.getClassLogger();

    // Cap iteration count for the cross-region block-event drain to prevent pathological loops.
    // Vanilla bounds piston/observer chains by physics; 8 passes is comfortably above what real
    // contraptions need.
    private static final int MAX_BLOCK_EVENT_PASSES = 8;

    private static final ThreadLocal<LevelChunkRegion> currentlyTickingRegion = new ThreadLocal<>();

    private final ServerChunkCache serverChunkCache;

    private final List<Entity> trackedEntitiesWorkerList = new ArrayList<>(); // Re-usable list for processing tracked entities in parallel

    public ShreddedPaperChunkTicker(ServerChunkCache serverChunkCache) {
        this.serverChunkCache = serverChunkCache;
    }

    public CompletableFuture<Void> tickChunks(final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
        ServerLevel level = this.serverChunkCache.chunkMap.level;

        // Phase A: per-region prep + block/fluid/random ticks (no block events).
        CompletableFuture<Void> future = scheduleAllRegions(level, region ->
                this._tickRegionPhaseA(level, region, timeInhabited, filteredSpawningCategories, spawnState));

        // Phase B: drain block events across all regions, looping until no cross-region propagation
        // remains. This barrier matches vanilla's single global runBlockEvents loop, so a piston
        // that triggers another piston in a neighbouring region resolves in the same game tick
        // regardless of the order regions were ticked.
        future = future.thenCompose(v -> drainBlockEventsAcrossRegions(level, MAX_BLOCK_EVENT_PASSES));

        // Phase C: per-region entity / block-entity / player ticks and broadcast.
        future = future.thenCompose(v -> scheduleAllRegions(level, region ->
                this._tickRegionPhaseC(level, region)));

        if (ShreddedPaperConfiguration.get().optimizations.processTrackQueueInParallel) future = future.thenCompose(v -> this.processTrackQueueInParallel(level));

        if (ShreddedPaperConfiguration.get().optimizations.flushQueueInParallel) future = future.thenCompose(v -> this.flushQueueInParallel(level));

        return future;
    }

    private CompletableFuture<Void> scheduleAllRegions(final ServerLevel level, final Consumer<LevelChunkRegion> action) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        level.chunkSource.tickingRegions.forEach(region ->
                futures.add(level.chunkScheduler.schedule(region.getRegionPos(), () -> action.accept(region)).exceptionally(e -> {
                    LogUtils.getClassLogger().error("Exception ticking region {}", region.getRegionPos(), e);
                    MinecraftServer.getServer().moonrise$setChunkSystemCrash(new RuntimeException("Ticking thread crash while ticking region " + region.getRegionPos(), e));
                    return null;
                }))
        );
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> drainBlockEventsAcrossRegions(final ServerLevel level, final int passesRemaining) {
        if (!level.tickRateManager().runsNormally()) {
            return CompletableFuture.completedFuture(null);
        }

        return scheduleAllRegions(level, region -> this._tickRegionPhaseB(level, region))
                .thenCompose(v -> {
                    if (passesRemaining <= 1) return CompletableFuture.completedFuture(null);
                    if (!anyRegionHasProcessableBlockEvents(level)) return CompletableFuture.completedFuture(null);
                    return drainBlockEventsAcrossRegions(level, passesRemaining - 1);
                });
    }

    private boolean anyRegionHasProcessableBlockEvents(final ServerLevel level) {
        AtomicBoolean found = new AtomicBoolean(false);
        level.chunkSource.tickingRegions.forEach(region -> {
            if (!found.get() && region.hasProcessableBlockEvents(level)) {
                found.set(true);
            }
        });
        return found.get();
    }

    /** processTrackQueue has been renamed to newTrackerTick */
    private CompletableFuture<Void> processTrackQueueInParallel(ServerLevel level) {
        level.getChunkSource().mainThreadProcessor.managedBlock(() -> level.chunkScheduler.getRegionLocker().globalLock().tryWriteLock() != 0);
        CompletableFuture<Void> allFuture = CompletableFuture.completedFuture(null);
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            trackedEntitiesWorkerList.clear();
            level.chunkSource.tickingRegions.forEach(
                    region -> region.forEachTrackedEntity(trackedEntitiesWorkerList::add)
            );

            List<List<Entity>> trackedEntitiesTasks = Lists.partition(trackedEntitiesWorkerList, Math.max(1, trackedEntitiesWorkerList.size() / ShreddedPaperTickThread.THREAD_COUNT / 3));
            for (List<Entity> trackedEntities : trackedEntitiesTasks) {
                if (trackedEntities.isEmpty()) continue;
                futures.add(CompletableFuture.runAsync(() -> trackedEntities.forEach(ShreddedPaperEntityTicker::processTrackQueue), ShreddedPaperTickThread.getExecutor()));
            }

            allFuture = allFuture.thenCompose(v -> CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)));
            return allFuture;
        } finally {
            allFuture.whenComplete((v, e) -> level.chunkScheduler.getRegionLocker().globalLock().tryUnlockWrite());
        }
    }

    private CompletableFuture<Void> flushQueueInParallel(ServerLevel level) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        List<List<ServerPlayer>> playersTasks = Lists.partition(new ArrayList<>(level.players()), Math.max(1, level.players().size() / ShreddedPaperTickThread.THREAD_COUNT / 3));
        for (List<ServerPlayer> players : playersTasks) {
            if (players.isEmpty()) continue;
            futures.add(CompletableFuture.runAsync(() -> players.forEach(player -> player.connection.connection.flushQueue()), ShreddedPaperTickThread.getExecutor()));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    public static boolean isCurrentlyTickingRegion(Level level, RegionPos regionPos) {
        LevelChunkRegion region = currentlyTickingRegion.get();
        return region != null && level.equals(region.getLevel()) && regionPos.equals(region.getRegionPos());
    }

    private void _tickRegionPhaseA(final ServerLevel level, final LevelChunkRegion region, final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
        try {
            currentlyTickingRegion.set(region);

            if (!(ShreddedPaperTickThread.isShreddedPaperTickThread())) {
                throw new IllegalStateException("Ticking region " + WorldUtil.getWorldName(level) + " " + region.getRegionPos() + " outside of ShreddedPaperTickThread!");
            }

            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            while (region.getInternalTaskQueue().executeTask()) ;

            level.moonrise$getChunkTaskScheduler().chunkHolderManager.processUnloads(region);

            region.forEachTickingEntity(entity -> {
                CraftEntity bukkitEntity = entity.getBukkitEntityRaw();
                if (bukkitEntity != null && !entity.isRemoved()) { // Entity could have been removed by another entity's task
                    bukkitEntity.taskScheduler.executeTick();
                }
            });

            region.tickTasks();

            if (level.tickRateManager().runsNormally()) {
                level.handlingTickThreadLocal.set(true);

                level.blockTicks.tick(region.getRegionPos(), level.getGameTime(), level.paperConfig().environment.maxBlockTicks, level::tickBlock);
                level.fluidTicks.tick(region.getRegionPos(), level.getGameTime(), level.paperConfig().environment.maxBlockTicks, level::tickFluid);

                region.forEach(chunk -> this._tickChunk(region, level, chunk, timeInhabited, filteredSpawningCategories, spawnState));

                level.handlingTickThreadLocal.set(false);
            }

            // Flush per-thread broadcaster while we still hold this region's locks; any blockChanged
            // calls that happened in Phase A must be sent before we release the region.
            ShreddedPaperChangesBroadcaster.broadcastChanges();
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickRegionPhaseB(final ServerLevel level, final LevelChunkRegion region) {
        try {
            currentlyTickingRegion.set(region);
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            level.handlingTickThreadLocal.set(true);
            level.runBlockEvents(region);
            level.handlingTickThreadLocal.set(false);

            ShreddedPaperChangesBroadcaster.broadcastChanges();
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickRegionPhaseC(final ServerLevel level, final LevelChunkRegion region) {
        try {
            currentlyTickingRegion.set(region);
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            region.forEachTickingEntity(ShreddedPaperEntityTicker::tickEntity);

            if (!ShreddedPaperConfiguration.get().optimizations.processTrackQueueInParallel) region.forEachTrackedEntity(ShreddedPaperEntityTicker::processTrackQueue);

            level.tickBlockEntities(region.tickingBlockEntities, region.pendingBlockEntityTickers);

            region.getPlayers().forEach(ShreddedPaperPlayerTicker::tickPlayer);

            while (region.getInternalTaskQueue().executeTask()) ;

            ShreddedPaperChangesBroadcaster.broadcastChanges();

            if (region.isEmpty()) {
                level.chunkSource.tickingRegions.remove(region.getRegionPos());
            }
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickChunk(final LevelChunkRegion levelChunkRegion, final ServerLevel world, final LevelChunk levelChunk, final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
        if (levelChunk.moonrise$getChunkHolder().vanillaChunkHolder.hasChangesToBroadcast())
            ShreddedPaperChangesBroadcaster.add(levelChunk.moonrise$getChunkHolder().vanillaChunkHolder); // ShreddedPaper

        // ShreddedPaper start - clear chunk packet cache
        if (levelChunk.cachedChunkPacket != null && levelChunk.cachedChunkPacketLastAccessed < world.getGameTime() - ShreddedPaperConfiguration.get().optimizations.chunkPacketCaching.expireAfter) {
            levelChunk.cachedChunkPacket = null;
        }
        // ShreddedPaper end - clear chunk packet cache

        if (!levelChunk.moonrise$getChunkHolder().isEntityTickingReady()) {
            return;
        }

        if (spawnState != null && levelChunkRegion.isPlayerTickingRequested(levelChunk.getPos())) {
            this._tickSpawningChunk(world, levelChunk, timeInhabited, filteredSpawningCategories, spawnState);
        }

        final int randomTickSpeed = world.getGameRules().get(net.minecraft.world.level.gamerules.GameRules.RANDOM_TICK_SPEED);
        world.tickChunk(levelChunk, randomTickSpeed);
    }

    private void _tickSpawningChunk(final ServerLevel world, final LevelChunk levelChunk, final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
        if (!world.chunkSource.chunkMap.isChunkNearPlayer(world.chunkSource.chunkMap, levelChunk.getPos(), levelChunk)) {
            return;
        }

        world.chunkSource.tickSpawningChunk(levelChunk, timeInhabited, filteredSpawningCategories, spawnState);
    }

    public static boolean willTrySpawnMobsThisTick(final ServerLevel level) {
        for (MobCategory mobCategory : NaturalSpawner.SPAWNING_CATEGORIES) {
            SpawnCategory spawnCategory = CraftSpawnCategory.toBukkit(mobCategory);
            if (CraftSpawnCategory.isValidForLimits(spawnCategory)) {
                if (level.ticksPerSpawnCategory.getLong(spawnCategory) != 0 && level.getLevelData().getGameTime() % level.ticksPerSpawnCategory.getLong(spawnCategory) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

}
