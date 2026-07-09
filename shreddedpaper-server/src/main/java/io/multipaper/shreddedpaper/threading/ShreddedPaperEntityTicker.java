package io.multipaper.shreddedpaper.threading;

import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.FullChunkStatus;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.util.profiling.Profiler;
import net.minecraft.util.profiling.ProfilerFiller;
import net.minecraft.world.entity.Entity;

import java.util.Objects;

public class ShreddedPaperEntityTicker {

    public static void tickEntity(Entity entity) {
        // ShreddedPaper start - dedupe ticks for entities that migrate across a region boundary mid-tick.
        // When an entity moves into a neighbouring region during its own tick, onSectionChange synchronously
        // reassigns it to the destination region's ticking list. Because adjacent regions tick sequentially
        // (their 3x3 locks overlap), the destination region - if it ticks later in this same pass - would
        // tick the entity a second time, applying physics/movement twice in one game tick. This is what
        // breaks tick-precise contraptions like minecart TNT dupers at region boundaries. Stamp the entity
        // with the current server tick and skip if it has already been ticked this tick.
        if (entity.shreddedPaperLastTickedTick == MinecraftServer.currentTick) {
            return;
        }
        entity.shreddedPaperLastTickedTick = MinecraftServer.currentTick;

        // ShreddedPaper - also dedupe vehicles (minecarts, boats, etc.) that migrate across region boundaries.
        // A vehicle could be ticked twice - once in the source region and once in the destination region
        // before the passenger is ticked. The vehicle's deduplication prevents it from losing its passenger
        // mid-tick and breaking contraptions.
        Entity vehicle = entity.getVehicle();
        if (vehicle != null && vehicle.shreddedPaperLastTickedTick == MinecraftServer.currentTick) {
            return;
        }
        vehicle.shreddedPaperLastTickedTick = MinecraftServer.currentTick;

        // ShreddedPaper end

        ProfilerFiller profilerFiller = Profiler.get();
        ServerLevel level = (ServerLevel) entity.level();

        if (!entity.isRemoved()) {
            if (!level.tickRateManager().isEntityFrozen(entity)) {
                profilerFiller.push("checkDespawn");
                entity.checkDespawn();
                profilerFiller.pop();
                if (true) { // Paper - rewrite chunk system
                    vehicle = entity.getVehicle();
                    if (vehicle != null) {
                        if (!vehicle.isRemoved() && vehicle.hasPassenger(entity)) {
                            return;
                        }

                        entity.stopRiding();
                    }

                    profilerFiller.push("tick");
                    level.guardEntityTick(level::tickNonPassenger, entity);
                    profilerFiller.pop();
                }
            }
        }
    }

    /** processTrackQueue has been renamed to newTrackerTick */
    public static void processTrackQueue(Entity entity) {
        ChunkMap.TrackedEntity tracker = Objects.requireNonNull(entity.moonrise$getTrackedEntity());
        ((ca.spottedleaf.moonrise.patches.entity_tracker.EntityTrackerTrackedEntity)tracker).moonrise$tick(((ca.spottedleaf.moonrise.patches.chunk_system.entity.ChunkSystemEntity)entity).moonrise$getChunkData().nearbyPlayers);
        if (((ca.spottedleaf.moonrise.patches.entity_tracker.EntityTrackerTrackedEntity)tracker).moonrise$hasPlayers()
                || ((ca.spottedleaf.moonrise.patches.chunk_system.entity.ChunkSystemEntity)entity).moonrise$getChunkStatus().isOrAfter(FullChunkStatus.ENTITY_TICKING)) {
            tracker.serverEntity.sendChanges();
        }
    }
}
