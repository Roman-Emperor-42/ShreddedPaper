package io.multipaper.shreddedpaper.util;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.waypoints.ServerWaypointManager;
import net.minecraft.world.waypoints.WaypointTransmitter;

import java.util.Set;

public class NoOpServerWaypointManager extends ServerWaypointManager {

    public NoOpServerWaypointManager(ServerLevel serverLevel) {
        super(serverLevel);
    }

    @Override
    public void trackWaypoint(WaypointTransmitter waypoint) {
        // Do nothing
    }

    @Override
    public void updateWaypoint(WaypointTransmitter waypoint) {
        // Do nothing
    }

    @Override
    public void untrackWaypoint(WaypointTransmitter waypoint) {
        // Do nothing
    }

    @Override
    public void addPlayer(ServerPlayer player) {
        // Do nothing
    }

    @Override
    public void updatePlayer(ServerPlayer player) {
        // Do nothing
    }

    @Override
    public void removePlayer(ServerPlayer player) {
        // Do nothing
    }

    @Override
    public void breakAllConnections() {
        // Do nothing
    }

    @Override
    public void remakeConnections(WaypointTransmitter waypoint) {
        // Do nothing
    }

    @Override
    public Set<WaypointTransmitter> transmitters() {
        return Set.of(); // Do nothing
    }

}
