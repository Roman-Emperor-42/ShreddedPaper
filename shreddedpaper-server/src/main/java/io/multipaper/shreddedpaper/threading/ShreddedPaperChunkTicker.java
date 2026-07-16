package io.multipaper.shreddedpaper.threading;

import ca.spottedleaf.moonrise.common.util.WorldUtil;
import com.google.common.collect.Lists;
import com.mojang.logging.LogUtils;
import io.multipaper.shreddedpaper.config.ShreddedPaperConfiguration;
import io.multipaper.shreddedpaper.region.RegionPos;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerChunkCache;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.MobCategory;
import net.minecraft.world.level.BlockEventData;
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
import java.util.Set;
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

        CompletableFuture<Void> future;
        if (ShreddedPaperConfiguration.get().optimizations.splitTickPhases) {
            // Regions with recent block-event activity near a shared border get their order-sensitive
            // tick phases (scheduled ticks, block events, block entities) run merged on one thread, in
            // vanilla's global ordering. A contraption straddling a region boundary depends on that
            // ordering end-to-end: moving-piston block entities finishing (C2) schedule observer pulses,
            // whose subtick order decides the scheduled-tick order (A), which decides the piston block
            // event order (B) - a random cross-region order at any of those stages tears the machine
            // apart. Clusters are >= 3 regions apart, so everything else still runs in parallel.
            final List<List<LevelChunkRegion>> mergedClusters = new ArrayList<>();
            final Set<LevelChunkRegion> mergedClusterMembers = new ReferenceOpenHashSet<>();
            final long gameTime = level.getGameTime();
            List<LevelChunkRegion> activeRegions = new ArrayList<>();
            level.chunkSource.tickingRegions.forEach(region -> {
                if (region.hasRecentBlockEventActivity(gameTime)) activeRegions.add(region);
            });
            for (List<LevelChunkRegion> cluster : clusterInteractingRegions(activeRegions)) {
                if (cluster.size() > 1) {
                    mergedClusters.add(cluster);
                    mergedClusterMembers.addAll(cluster);
                }
            }

            // Phase A: per-region prep + block/fluid/random ticks (no block events).
            future = scheduleAllRegionsClustered(level, mergedClusters, mergedClusterMembers,
                    region -> this._tickRegionPhaseA(level, region, timeInhabited, filteredSpawningCategories, spawnState),
                    cluster -> this._tickClusterPhaseA(level, cluster, timeInhabited, filteredSpawningCategories, spawnState));

            // Phase B: drain block events across all regions. This barrier matches vanilla's single global
            // runBlockEvents loop, so a piston that triggers another piston in a neighbouring region resolves
            // in the same game tick regardless of the order regions were ticked. Regions close enough for
            // their events to interact are drained by one task in global queue order - vanilla's single-FIFO
            // order - because multi-engine contraptions straddling a region boundary (e.g. spindle trenchers)
            // tear apart if the two engines' piston events run in a random relative order each tick.
            future = future.thenCompose(v -> drainBlockEventsAcrossRegions(level, MAX_BLOCK_EVENT_PASSES));

            // Phase C1: entity ticks across all regions. Vanilla ticks ALL entities before ANY block
            // entities; this barrier keeps that global order so an entity riding a contraption that
            // straddles a region boundary (e.g. a minecart in a flying machine bucket) never collides
            // against moving-piston shapes that have already advanced this tick - per-region fused
            // ordering flips randomly at the boundary and lets the entity sink out of the machine.
            future = future.thenCompose(v -> scheduleAllRegions(level, region ->
                    this._tickRegionPhaseC1(level, region)));

            // Phase C2: per-region block-entity / player ticks and broadcast. Clustered regions
            // tick their block entities merged by registration order (vanilla's global list order).
            future = future.thenCompose(v -> scheduleAllRegionsClustered(level, mergedClusters, mergedClusterMembers,
                    region -> this._tickRegionPhaseC2(level, region),
                    cluster -> this._tickClusterPhaseC2(level, cluster)));
        } else {
            // Single fused pass per region; cross-region redstone may break.
            future = scheduleAllRegions(level, region ->
                    this._tickRegionFused(level, region, timeInhabited, filteredSpawningCategories, spawnState));
        }

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

    /**
     * Like {@link #scheduleAllRegions}, but regions belonging to a cluster are handled by a single
     * task per cluster (holding all members' locks) so the cluster action can run the members'
     * order-sensitive work merged in vanilla order.
     */
    private CompletableFuture<Void> scheduleAllRegionsClustered(final ServerLevel level, final List<List<LevelChunkRegion>> clusters,
                                                                final Set<LevelChunkRegion> clusterMembers, final Consumer<LevelChunkRegion> action,
                                                                final Consumer<List<LevelChunkRegion>> clusterAction) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        level.chunkSource.tickingRegions.forEach(region -> {
            if (clusterMembers.contains(region)) return;
            futures.add(level.chunkScheduler.schedule(region.getRegionPos(), () -> action.accept(region)).exceptionally(e -> {
                LogUtils.getClassLogger().error("Exception ticking region {}", region.getRegionPos(), e);
                MinecraftServer.getServer().moonrise$setChunkSystemCrash(new RuntimeException("Ticking thread crash while ticking region " + region.getRegionPos(), e));
                return null;
            }));
        });
        for (List<LevelChunkRegion> cluster : clusters) {
            RegionPos firstRegionPos = cluster.get(0).getRegionPos();
            futures.add(level.chunkScheduler.scheduleOnMany(() -> clusterAction.accept(cluster), cluster.stream().map(LevelChunkRegion::getRegionPos).toArray(RegionPos[]::new)).exceptionally(e -> {
                LogUtils.getClassLogger().error("Exception ticking region cluster at {}", firstRegionPos, e);
                MinecraftServer.getServer().moonrise$setChunkSystemCrash(new RuntimeException("Ticking thread crash while ticking region cluster at " + firstRegionPos, e));
                return null;
            }));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> drainBlockEventsAcrossRegions(final ServerLevel level, final int passesRemaining) {
        if (!level.tickRateManager().runsNormally()) {
            return CompletableFuture.completedFuture(null);
        }

        // Only regions with a processable event need a drain task this pass - on ticks where no
        // piston/observer fired anywhere, the entire Phase B barrier is skipped.
        List<LevelChunkRegion> regionsWithEvents = new ArrayList<>();
        level.chunkSource.tickingRegions.forEach(region -> {
            if (region.hasProcessableBlockEvents(level)) regionsWithEvents.add(region);
        });
        if (regionsWithEvents.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Group regions whose events can interact this pass. An event runs under its region's 3x3
        // write lock and only mutates blocks within one region of home, so two regions' events can
        // affect each other only within Chebyshev distance 2. Each such cluster is drained by a
        // single task that merges the member queues by global sequence, reproducing vanilla's
        // single-FIFO block event order for contraptions that straddle a region boundary. Distinct
        // clusters are >= 3 regions apart, so their lock zones are disjoint and they run in parallel.
        List<List<LevelChunkRegion>> clusters = clusterInteractingRegions(regionsWithEvents);

        List<CompletableFuture<Void>> futures = new ArrayList<>(clusters.size());
        for (List<LevelChunkRegion> cluster : clusters) {
            Runnable task = () -> this._drainClusterBlockEvents(level, cluster);
            CompletableFuture<Void> future;
            if (cluster.size() == 1) {
                future = level.chunkScheduler.schedule(cluster.get(0).getRegionPos(), task);
            } else {
                future = level.chunkScheduler.scheduleOnMany(task, cluster.stream().map(LevelChunkRegion::getRegionPos).toArray(RegionPos[]::new));
            }
            futures.add(future.exceptionally(e -> {
                RegionPos regionPos = cluster.get(0).getRegionPos();
                LogUtils.getClassLogger().error("Exception draining block events for region cluster at {}", regionPos, e);
                MinecraftServer.getServer().moonrise$setChunkSystemCrash(new RuntimeException("Ticking thread crash while draining block events for region cluster at " + regionPos, e));
                return null;
            }));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenCompose(v -> {
                    if (passesRemaining <= 1) return CompletableFuture.completedFuture(null);
                    if (!anyRegionHasProcessableBlockEvents(level)) return CompletableFuture.completedFuture(null);
                    return drainBlockEventsAcrossRegions(level, passesRemaining - 1);
                });
    }

    /**
     * Groups the given regions into clusters of regions whose block events can interact this
     * pass (any two members within Chebyshev distance 2 of each other, transitively).
     */
    private static List<List<LevelChunkRegion>> clusterInteractingRegions(List<LevelChunkRegion> regions) {
        List<List<LevelChunkRegion>> clusters = new ArrayList<>();
        for (LevelChunkRegion region : regions) {
            List<LevelChunkRegion> home = null;
            for (int i = 0; i < clusters.size(); i++) {
                List<LevelChunkRegion> cluster = clusters.get(i);
                if (!canInteract(region, cluster)) continue;
                if (home == null) {
                    cluster.add(region);
                    home = cluster;
                } else {
                    // This region bridges two clusters - merge them
                    home.addAll(cluster);
                    clusters.remove(i);
                    i--;
                }
            }
            if (home == null) {
                List<LevelChunkRegion> cluster = new ArrayList<>(2);
                cluster.add(region);
                clusters.add(cluster);
            }
        }
        return clusters;
    }

    private static boolean canInteract(LevelChunkRegion region, List<LevelChunkRegion> cluster) {
        for (LevelChunkRegion other : cluster) {
            if (Math.abs(region.getRegionPos().x - other.getRegionPos().x) <= 2
                    && Math.abs(region.getRegionPos().z - other.getRegionPos().z) <= 2) {
                return true;
            }
        }
        return false;
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

    /**
     * Phase A for a cluster of interacting redstone-active regions: per-member prep, then the
     * members' scheduled block/fluid ticks merged in vanilla's global (priority, subtick) order,
     * then per-member chunk ticks. Holding all members on one thread is what allows observers
     * firing on both sides of a region boundary in the same tick to fire in vanilla order.
     */
    private void _tickClusterPhaseA(final ServerLevel level, final List<LevelChunkRegion> cluster, final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
        try {
            if (!(ShreddedPaperTickThread.isShreddedPaperTickThread())) {
                throw new IllegalStateException("Ticking region cluster " + WorldUtil.getWorldName(level) + " " + cluster.get(0).getRegionPos() + " outside of ShreddedPaperTickThread!");
            }

            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            for (LevelChunkRegion region : cluster) {
                currentlyTickingRegion.set(region);

                while (region.getInternalTaskQueue().executeTask()) ;

                level.moonrise$getChunkTaskScheduler().chunkHolderManager.processUnloads(region);

                region.forEachTickingEntity(entity -> {
                    CraftEntity bukkitEntity = entity.getBukkitEntityRaw();
                    if (bukkitEntity != null && !entity.isRemoved()) { // Entity could have been removed by another entity's task
                        bukkitEntity.taskScheduler.executeTick();
                    }
                });

                region.tickTasks();
            }

            if (level.tickRateManager().runsNormally()) {
                level.handlingTickThreadLocal.set(true);

                List<RegionPos> regionPositions = cluster.stream().map(LevelChunkRegion::getRegionPos).toList();
                level.blockTicks.tickMerged(regionPositions, level.getGameTime(), level.paperConfig().environment.maxBlockTicks,
                        (pos, block) -> {
                            currentlyTickingRegion.set(regionForBlockPos(cluster, pos));
                            level.tickBlock(pos, block);
                        });
                level.fluidTicks.tickMerged(regionPositions, level.getGameTime(), level.paperConfig().environment.maxBlockTicks,
                        (pos, fluid) -> {
                            currentlyTickingRegion.set(regionForBlockPos(cluster, pos));
                            level.tickFluid(pos, fluid);
                        });

                for (LevelChunkRegion region : cluster) {
                    currentlyTickingRegion.set(region);
                    region.forEach(chunk -> this._tickChunk(region, level, chunk, timeInhabited, filteredSpawningCategories, spawnState));
                }

                level.handlingTickThreadLocal.set(false);
            }

            ShreddedPaperChangesBroadcaster.broadcastChanges();
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private static LevelChunkRegion regionForBlockPos(final List<LevelChunkRegion> cluster, final net.minecraft.core.BlockPos pos) {
        long regionKey = RegionPos.forBlockPos(pos).longKey;
        for (LevelChunkRegion region : cluster) {
            if (region.getRegionPos().longKey == regionKey) return region;
        }
        return cluster.get(0);
    }

    /**
     * Drains the block event queues of a cluster of interacting regions on a single thread,
     * always running the queued event with the lowest global sequence next. This is exactly
     * vanilla's single-FIFO drain restricted to the cluster: events queued during processing
     * (into any member region) are picked up in the same sweep, and events cascading into
     * regions outside the cluster are handled by the next barrier pass.
     */
    private void _drainClusterBlockEvents(final ServerLevel level, final List<LevelChunkRegion> cluster) {
        try {
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();
            level.handlingTickThreadLocal.set(true);

            List<LevelChunkRegion> rescheduleRegions = null;
            List<BlockEventData> rescheduleEvents = null;
            LongArrayList rescheduleSeqs = null;

            while (true) {
                LevelChunkRegion bestRegion = null;
                long bestSeq = Long.MAX_VALUE;
                for (LevelChunkRegion region : cluster) {
                    long seq = region.peekFirstBlockEventSeq();
                    if (seq < bestSeq) {
                        bestSeq = seq;
                        bestRegion = region;
                    }
                }
                if (bestRegion == null) break;

                BlockEventData blockEventData = bestRegion.removeFirstBlockEvent();
                currentlyTickingRegion.set(bestRegion);
                if (!level.runQueuedBlockEvent(blockEventData)) {
                    // Stash unprocessable events and re-queue them after the drain (with their
                    // original sequence), like vanilla's blockEventsToReschedule.
                    if (rescheduleRegions == null) {
                        rescheduleRegions = new ArrayList<>();
                        rescheduleEvents = new ArrayList<>();
                        rescheduleSeqs = new LongArrayList();
                    }
                    rescheduleRegions.add(bestRegion);
                    rescheduleEvents.add(blockEventData);
                    rescheduleSeqs.add(bestSeq);
                }
            }

            if (rescheduleRegions != null) {
                for (int i = 0; i < rescheduleRegions.size(); i++) {
                    rescheduleRegions.get(i).addBlockEventWithSeq(rescheduleEvents.get(i), rescheduleSeqs.getLong(i));
                }
            }

            level.handlingTickThreadLocal.set(false);
            ShreddedPaperChangesBroadcaster.broadcastChanges();
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickRegionPhaseC1(final ServerLevel level, final LevelChunkRegion region) {
        try {
            currentlyTickingRegion.set(region);
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            region.forEachTickingEntity(ShreddedPaperEntityTicker::tickEntity);

            // Flush while we still hold this region's locks; any blockChanged calls from entity
            // ticks must be sent before we release the region.
            ShreddedPaperChangesBroadcaster.broadcastChanges();
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickRegionPhaseC2(final ServerLevel level, final LevelChunkRegion region) {
        try {
            currentlyTickingRegion.set(region);
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

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

    /**
     * Phase C2 for a cluster of interacting redstone-active regions. The members' block
     * entities tick merged by registration order (vanilla's global list order), so two
     * moving-piston block entities on opposite sides of a region boundary finish - and
     * schedule their observers' pulses - in the same relative order as vanilla.
     */
    private void _tickClusterPhaseC2(final ServerLevel level, final List<LevelChunkRegion> cluster) {
        try {
            ShreddedPaperChangesBroadcaster.setAsWorkerThread();

            if (!ShreddedPaperConfiguration.get().optimizations.processTrackQueueInParallel) {
                for (LevelChunkRegion region : cluster) {
                    currentlyTickingRegion.set(region);
                    region.forEachTrackedEntity(ShreddedPaperEntityTicker::processTrackQueue);
                }
            }

            level.tickBlockEntitiesMerged(cluster, currentlyTickingRegion::set);

            for (LevelChunkRegion region : cluster) {
                currentlyTickingRegion.set(region);

                region.getPlayers().forEach(ShreddedPaperPlayerTicker::tickPlayer);

                while (region.getInternalTaskQueue().executeTask()) ;
            }

            ShreddedPaperChangesBroadcaster.broadcastChanges();

            for (LevelChunkRegion region : cluster) {
                if (region.isEmpty()) {
                    level.chunkSource.tickingRegions.remove(region.getRegionPos());
                }
            }
        } finally {
            currentlyTickingRegion.remove();
        }
    }

    private void _tickRegionFused(final ServerLevel level, final LevelChunkRegion region, final long timeInhabited, final List<MobCategory> filteredSpawningCategories, final NaturalSpawner.SpawnState spawnState) {
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
                if (bukkitEntity != null && !entity.isRemoved()) {
                    bukkitEntity.taskScheduler.executeTick();
                }
            });

            region.tickTasks();

            if (level.tickRateManager().runsNormally()) {
                level.handlingTickThreadLocal.set(true);

                level.blockTicks.tick(region.getRegionPos(), level.getGameTime(), level.paperConfig().environment.maxBlockTicks, level::tickBlock);
                level.fluidTicks.tick(region.getRegionPos(), level.getGameTime(), level.paperConfig().environment.maxBlockTicks, level::tickFluid);

                region.forEach(chunk -> this._tickChunk(region, level, chunk, timeInhabited, filteredSpawningCategories, spawnState));

                level.runBlockEvents(region);

                level.handlingTickThreadLocal.set(false);
            }

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
