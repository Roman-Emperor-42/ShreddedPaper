package io.multipaper.shreddedpaper.region;

import ca.spottedleaf.concurrentutil.executor.queue.PrioritisedTaskQueue;
import ca.spottedleaf.moonrise.common.list.IteratorSafeOrderedReferenceSet;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2LongLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.Mob;
import net.minecraft.world.level.BlockEventData;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.block.RedstoneTorchBlock;
import net.minecraft.world.level.block.entity.TickingBlockEntity;
import net.minecraft.world.level.chunk.LevelChunk;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

public class LevelChunkRegion {

    private final ServerLevel level;
    private final RegionPos regionPos;
    private final List<LevelChunk> levelChunks = new ArrayList<>(RegionPos.REGION_SIZE * RegionPos.REGION_SIZE);
    private final LongOpenHashSet playerTickingChunkRequests = new LongOpenHashSet(); // ChunkPos.longKey
    private final IteratorSafeOrderedReferenceSet<Entity> tickingEntities = new IteratorSafeOrderedReferenceSet<>(); // Use IteratorSafeOrderedReferenceSet to maintain entity tick order
    private final Set<Entity> trackedEntities = new ObjectOpenHashSet<>();
    private final ConcurrentLinkedQueue<DelayedTask> scheduledTasks = new ConcurrentLinkedQueue<>(); // Writable tasks
    private final PrioritisedTaskQueue internalTasks = new PrioritisedTaskQueue(); // Read-only tasks
    private final ObjectOpenHashSet<ServerPlayer> players = new ObjectOpenHashSet<>();
    public final LongLinkedOpenHashSet unloadQueue = new LongLinkedOpenHashSet();
    public final List<TickingBlockEntity> tickingBlockEntities = new ReferenceArrayList<>();
    public final List<TickingBlockEntity> pendingBlockEntityTickers = new ReferenceArrayList<>();
    private final ObjectOpenHashSet<Mob> navigatingMobs = new ObjectOpenHashSet<>();
    // Insertion-ordered, deduplicating like the vanilla ObjectLinkedOpenHashSet, but each event
    // also carries a level-global sequence number so the split-phase block-event drain can merge
    // the queues of adjacent regions back into vanilla's single-FIFO processing order.
    private final Object2LongLinkedOpenHashMap<BlockEventData> blockEvents = new Object2LongLinkedOpenHashMap<>();
    private volatile long lastAccessTick;
    // Neighbouring regions (by RegionPos long key) near whose shared border an order-sensitive
    // (piston-family) block event was recently queued, mapped to the game time of the last such
    // event. While an edge is fresh, the split-phase ticker merges the two regions' tick phases
    // so a contraption working across that border sees vanilla ordering. Guarded by this region's
    // monitor. See ShreddedPaperChunkTicker.
    private final Long2LongOpenHashMap activeBorderEdges = new Long2LongOpenHashMap();
    public ArrayDeque<RedstoneTorchBlock.Toggle> redstoneUpdateInfos;

    public LevelChunkRegion(ServerLevel level, RegionPos regionPos) {
        this.level = level;
        this.regionPos = regionPos;

        this.bumpLastAccess();
    }

    public void bumpLastAccess() {
        this.lastAccessTick = this.level.levelData.getGameTime();
    }

    public synchronized void add(LevelChunk levelChunk) {
        this.levelChunks.add(levelChunk);
    }

    public synchronized void remove(LevelChunk levelChunk) {
        if (!this.levelChunks.remove(levelChunk)) {
            throw new IllegalStateException("Tried to remove a chunk that wasn't in the region: " + levelChunk.getPos());
        }
    }

    public synchronized void addPlayerTickingRequest(final ChunkPos chunkPos) {
        this.playerTickingChunkRequests.add(chunkPos.toLong());
    }

    public synchronized void removePlayerTickingRequest(final ChunkPos chunkPos) {
        this.playerTickingChunkRequests.remove(chunkPos.toLong());
    }

    public boolean isPlayerTickingRequested(final ChunkPos chunkPos) {
        if (chunkPos.getRegionPos().toLong() != this.regionPos.toLong()) {
            throw new IllegalStateException("Chunk %s is not in region %s".formatted(chunkPos, this.regionPos));
        }

        return this.playerTickingChunkRequests.contains(chunkPos.toLong());
    }

    public synchronized void addTickingEntity(Entity entity) {
        if (!this.tickingEntities.add(entity)) {
            throw new IllegalStateException("Tried to add an entity that was already in the ticking list: " + entity);
        }
    }

    public synchronized void removeTickingEntity(Entity entity) {
        if (!this.tickingEntities.remove(entity)) {
            throw new IllegalStateException("Tried to remove an entity that wasn't in the ticking list: " + entity);
        }
    }

    public void forEachTickingEntity(Consumer<Entity> action) {
        IteratorSafeOrderedReferenceSet.Iterator<Entity> iterator = this.tickingEntities.iterator();
        try {
            while (iterator.hasNext()) {
                action.accept(iterator.next());
            }
        } finally {
            iterator.finishedIterating();
        }
    }

    public synchronized void addTrackedEntity(Entity entity) {
        if (!this.trackedEntities.add(entity)) {
            throw new IllegalStateException("Tried to add an entity that was already tracked: " + entity);
        }
    }

    public synchronized void removeTrackedEntity(Entity entity) {
        if (!this.trackedEntities.remove(entity)) {
            throw new IllegalStateException("Tried to remove an entity that wasn't already tracked: " + entity);
        }
    }

    public synchronized void forEachTrackedEntity(Consumer<Entity> action) {
        this.trackedEntities.forEach(action);
    }

    public ServerLevel getLevel() {
        return this.level;
    }

    public void scheduleTask(Runnable task, long delay) {
        this.scheduledTasks.add(new DelayedTask(task, delay));
    }

    public PrioritisedTaskQueue getInternalTaskQueue() {
        return this.internalTasks;
    }

    public synchronized void addPlayer(ServerPlayer player) {
        if (!this.players.add(player)) {
            throw new IllegalStateException("Tried to add a player that was already in the region: " + player.getUUID());
        }
    }

    public synchronized void removePlayer(ServerPlayer player) {
        if (!this.players.remove(player)) {
            throw new IllegalStateException("Tried to remove a player that wasn't in the region: " + player.getUUID());
        }
    }

    public synchronized List<ServerPlayer> getPlayers() {
        return this.players.isEmpty() ? List.of() : new ObjectArrayList<>(this.players);
    }

    public synchronized void addNavigationMob(Mob mob) {
        this.navigatingMobs.add(mob);
    }

    public synchronized void removeNavigationMob(Mob mob) {
        this.navigatingMobs.remove(mob);
    }

    public synchronized void collectNavigatingMobs(List<Mob> collection) {
        collection.addAll(this.navigatingMobs);
    }

    public RegionPos getRegionPos() {
        return regionPos;
    }

    public void forEach(Consumer<LevelChunk> consumer) {
        // This method has the chance of skipping a chunk if a chunk is removed via another thread during this iteration
        for (int i = 0; i < this.levelChunks.size(); i++) {
            try {
                LevelChunk levelChunk = this.levelChunks.get(i);
                if (levelChunk != null) {
                    consumer.accept(levelChunk);
                }
            } catch (IndexOutOfBoundsException e) {
                // Ignore - multithreaded modification
            }
        }
    }

    public void tickTasks() {
        if (this.scheduledTasks.isEmpty()) return;

        List<DelayedTask> toRun = new ArrayList<>();
        for (DelayedTask task : this.scheduledTasks) {
            // Check if a task should run before executing the tasks, as tasks may add more tasks while they are running
            if (task.shouldRun()) {
                toRun.add(task);
            }
        }

        this.scheduledTasks.removeAll(toRun);
        toRun.forEach(DelayedTask::run);
    }

    public synchronized void addBlockEvent(BlockEventData blockEvent, AtomicLong seqCounter) {
        // Assign the sequence inside the region monitor so this queue stays sorted by sequence;
        // re-adding a duplicate keeps the original position and sequence, matching the vanilla
        // linked-set behaviour.
        if (!this.blockEvents.containsKey(blockEvent)) {
            this.blockEvents.put(blockEvent, seqCounter.getAndIncrement());
        }
    }

    /**
     * Re-queue an event that couldn't be processed this tick, keeping its original sequence
     * so it stays ahead of anything queued after it.
     */
    public synchronized void addBlockEventWithSeq(BlockEventData blockEvent, long seq) {
        if (!this.blockEvents.containsKey(blockEvent)) {
            this.blockEvents.put(blockEvent, seq);
        }
    }

    public boolean hasBlockEvents() {
        return !this.blockEvents.isEmpty();
    }

    /**
     * Returns true if this region has any queued block events whose position is currently
     * tickable. Used to decide whether the cross-region block-event drain loop should run
     * another pass — events whose chunks aren't tick-ready get rescheduled in place each
     * pass, so checking only those would loop forever.
     */
    public synchronized boolean hasProcessableBlockEvents(ServerLevel level) {
        if (this.blockEvents.isEmpty()) return false;
        for (BlockEventData event : this.blockEvents.keySet()) {
            if (level.shouldTickBlocksAt(event.pos())) return true;
        }
        return false;
    }

    /**
     * Sequence number of the oldest queued block event, or Long.MAX_VALUE if the queue is
     * empty. Concurrent adds only ever append (the sequence counter is monotonic), so the
     * head observed here can only be removed by the caller itself.
     */
    public synchronized long peekFirstBlockEventSeq() {
        return this.blockEvents.isEmpty() ? Long.MAX_VALUE : this.blockEvents.getLong(this.blockEvents.firstKey());
    }

    public synchronized BlockEventData removeFirstBlockEvent() {
        BlockEventData event = this.blockEvents.firstKey();
        this.blockEvents.removeLong(event);
        return event;
    }

    public synchronized void removeBlockEventsIf(Predicate<BlockEventData> predicate) {
        this.blockEvents.keySet().removeIf(predicate);
    }

    // The freshness window is a few ticks so contraptions with slow clocks stay merged between
    // firings, and covers the lifetime of the moving-piston block entities an event creates.
    private static final int BORDER_EDGE_ACTIVITY_WINDOW = 5;

    public synchronized void stampBorderEdge(long neighbourRegionKey, long gameTime) {
        this.activeBorderEdges.put(neighbourRegionKey, gameTime);
    }

    /**
     * Passes the region key of each neighbour with fresh border block-event activity to the
     * consumer, pruning stale edges as it goes.
     */
    public synchronized void forEachRecentBorderEdge(long gameTime, LongConsumer consumer) {
        if (this.activeBorderEdges.isEmpty()) return;
        ObjectIterator<Long2LongMap.Entry> iterator = this.activeBorderEdges.long2LongEntrySet().fastIterator();
        while (iterator.hasNext()) {
            Long2LongMap.Entry entry = iterator.next();
            if (gameTime - entry.getLongValue() <= BORDER_EDGE_ACTIVITY_WINDOW) {
                consumer.accept(entry.getLongKey());
            } else {
                iterator.remove();
            }
        }
    }

    public boolean isEmpty() {
        return this.lastAccessTick < this.level.levelData.getGameTime() - 20
                && levelChunks.isEmpty()
                && playerTickingChunkRequests.isEmpty()
                && tickingEntities.size() == 0
                && scheduledTasks.isEmpty()
                && internalTasks.getTotalTasksExecuted() >= internalTasks.getTotalTasksScheduled()
                && players.isEmpty()
                && unloadQueue.isEmpty()
                && tickingBlockEntities.isEmpty()
                && pendingBlockEntityTickers.isEmpty()
                && trackedEntities.isEmpty()
                && navigatingMobs.isEmpty()
                && blockEvents.isEmpty()
                ;
    }
}
